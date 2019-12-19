import time
from collections import OrderedDict

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.traced_data.util import FoldTracedData
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import TimeUtils

from src.lib import PipelineConfiguration, ConsentUtils
from src.lib.pipeline_configuration import CodingModes, FoldingModes


class AnalysisFile(object):
    @staticmethod
    def generate(user, data, csv_by_message_output_path, csv_by_individual_output_path):
        # Serializer is currently overflowing
        # TODO: Investigate/address the cause of this.
        # sys.setrecursionlimit(15000)

        consent_withdrawn_key = "consent_withdrawn"
        for td in data:
            td.append_data({consent_withdrawn_key: Codes.FALSE},
                           Metadata(user, Metadata.get_call_location(), time.time()))

        # Set the list of keys to be exported and how they are to be handled when folding
        fold_strategies = OrderedDict()
        fold_strategies["uid"] = FoldStrategies.assert_equal
        fold_strategies[consent_withdrawn_key] = FoldStrategies.boolean_or

        export_keys = ["uid", consent_withdrawn_key]

        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    export_keys.append(cc.analysis_file_key)

                    if cc.folding_mode == FoldingModes.ASSERT_EQUAL:
                        fold_strategies[cc.coded_field] = FoldStrategies.assert_label_ids_equal
                        fold_strategies[cc.analysis_file_key] = FoldStrategies.assert_equal
                    elif cc.folding_mode == FoldingModes.YES_NO_AMB:
                        assert False, "Folding yes/no/ambivalent codes not supported"
                    else:
                        assert False, f"Incompatible folding_mode {plan.folding_mode}"
                else:
                    assert cc.folding_mode == FoldingModes.MATRIX
                    for code in cc.code_scheme.codes:
                        export_keys.append(f"{cc.analysis_file_key}{code.string_value}")
                        fold_strategies[f"{cc.analysis_file_key}{code.string_value}"] = FoldStrategies.matrix
                        fold_strategies[cc.coded_field] = \
                            lambda x, y, code_scheme=cc.code_scheme: FoldStrategies.list_of_labels(code_scheme, x, y)

            export_keys.append(plan.raw_field)
            if plan.raw_field_folding_mode == FoldingModes.CONCATENATE:
                fold_strategies[plan.raw_field] = FoldStrategies.concatenate
            elif plan.raw_field_folding_mode == FoldingModes.ASSERT_EQUAL:
                fold_strategies[plan.raw_field] = FoldStrategies.assert_equal
            else:
                assert False, f"Incompatible raw_field_folding_mode {plan.raw_field_folding_mode}"

        # Convert codes to their string/matrix values
        for td in data:
            analysis_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.SINGLE:
                        analysis_dict[cc.analysis_file_key] = \
                            cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"]).string_value
                    else:
                        assert cc.coding_mode == CodingModes.MULTIPLE
                        show_matrix_keys = []
                        for code in cc.code_scheme.codes:
                            show_matrix_keys.append(f"{cc.analysis_file_key}{code.string_value}")

                        for label in td.get(cc.coded_field, []):
                            code_string_value = cc.code_scheme.get_code_with_code_id(label['CodeID']).string_value
                            analysis_dict[f"{cc.analysis_file_key}{code_string_value}"] = Codes.MATRIX_1

                        for key in show_matrix_keys:
                            if key not in analysis_dict:
                                analysis_dict[key] = Codes.MATRIX_0
            td.append_data(analysis_dict,
                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Set consent withdrawn based on presence of data coded as "stop"
        ConsentUtils.determine_consent_withdrawn(
            user, data, PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS,
            consent_withdrawn_key
        )

        # Fold data to have one respondent per row
        to_be_folded = []
        for td in data:
            to_be_folded.append(td.copy())

        folded_data = FoldTracedData.fold_iterable_of_traced_data(
            user, data, lambda td: td["uid"], fold_strategies
        )

        # Fix-up _NA and _NC keys, which are currently being set incorrectly by
        # FoldTracedData.fold_iterable_of_traced_data when there are multiple radio shows
        # TODO: Update FoldTracedData to handle NA and NC correctly under multiple radio shows
        #       This is probably best done by updating Core to support folding lists of labels, then updating this
        #       file to convert from labels to matrix representation and other string values after folding.
        for td in folded_data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.MULTIPLE:
                        if plan.raw_field in td:
                            td.append_data({f"{cc.analysis_file_key}{Codes.TRUE_MISSING}": Codes.MATRIX_0},
                                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

                        contains_non_nc_key = False
                        for code in cc.code_scheme.codes:
                            if td.get(f"{cc.analysis_file_key}{code.string_value}") == Codes.MATRIX_1 and \
                                    code.control_code != Codes.NOT_CODED:
                                contains_non_nc_key = True
                        if contains_non_nc_key:
                            td.append_data({f"{cc.analysis_file_key}{Codes.NOT_CODED}": Codes.MATRIX_0},
                                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
                        else:
                            td.append_data({f"{cc.analysis_file_key}{Codes.NOT_CODED}": Codes.MATRIX_1},
                                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Check that the new and old strategies of folding give the same response
        # TODO: Remove this when the old strategies are removed, as this will serve no purpose then.
        for td in folded_data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.SINGLE:
                        if cc.folding_mode == FoldingModes.ASSERT_EQUAL:
                            assert cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"]).string_value == \
                                td[cc.analysis_file_key]
                        # TODO: Check other folding_modes once implemented above and in Core Data
                    else:
                        assert cc.coding_mode == CodingModes.MULTIPLE
                        old_matrix_values = dict()
                        for code in cc.code_scheme.codes:
                            old_matrix_values[code.code_id] = td[f"{cc.analysis_file_key}{code.string_value}"]

                        new_matrix_values = dict()
                        for code in cc.code_scheme.codes:
                            new_matrix_values[code.code_id] = Codes.MATRIX_0
                        for label in td[cc.coded_field]:
                            new_matrix_values[label["CodeID"]] = Codes.MATRIX_1

                        assert new_matrix_values == old_matrix_values, f"{td['uid']}\n{old_matrix_values}\n{new_matrix_values}"

        # Process consent
        ConsentUtils.set_stopped(user, data, consent_withdrawn_key, additional_keys=export_keys)
        ConsentUtils.set_stopped(user, folded_data, consent_withdrawn_key, additional_keys=export_keys)

        # Output to CSV with one message per row
        with open(csv_by_message_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

        with open(csv_by_individual_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(folded_data, f, headers=export_keys)

        return data, folded_data
