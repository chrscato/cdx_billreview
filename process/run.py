# process/run.py

from process.utils.loader import load_claim_from_s3, extract_procedure_data
from process.utils.arthrogram_check import check_and_redirect_if_arthrogram
from process.utils.extract_procedures import extract_from_service_lines, extract_from_line_items
from process.utils.filter_ancillaries import filter_ancillaries
from process.utils.match_exact import match_exact
from process.utils.match_bundles import match_bundles
from process.utils.match_clinical_equiv import match_clinical_equiv
from process.utils.unit_check import validate_units
from process.utils.rate_check import validate_rates
from process.utils.validator_logger import log_validation_results


def run_claim_validation(file_name: str):
    # Step 1: Load claim JSON
    claim = load_claim_from_s3(file_name)

    # Step 2: Skip if bundle_type == arthrogram
    if check_and_redirect_if_arthrogram(file_name, claim):
        return

    # Step 3: Extract raw CPT lines from JSON
    billed_raw, ordered_raw, metadata = extract_procedure_data(claim)

    # Step 4: Normalize into Procedure objects
    billed = extract_from_service_lines(billed_raw)
    ordered = extract_from_line_items(ordered_raw)

    # Step 5: Filter out ancillary codes
    billed_filtered, skipped_codes = filter_ancillaries(billed)

    # Step 6: Run matching checks
    exact_matches, billed_remaining, ordered_remaining = match_exact(billed_filtered, ordered)
    bundle_matches, billed_remaining = match_bundles(billed_remaining)
    clinical_matches, billed_remaining = match_clinical_equiv(billed_remaining, ordered_remaining)
    all_matches = exact_matches + bundle_matches + clinical_matches

    # Step 7: Units check (flag >1 units on non-ancillaries)
    unit_flag, unit_messages = validate_units(billed_filtered)

    # Step 8: Rate validation (only if no prior failures)
    if not billed_remaining and not unit_flag:
        rate_failure, rate_map, missing_rates = validate_rates(billed_filtered, claim["filemaker"]["provider"]["TIN"])
    else:
        rate_failure = False
        missing_rates = []

    # Step 9: Log results + move JSON to correct output location
    log_validation_results(
        file_name=file_name,
        claim_json=claim,
        match_results=all_matches,
        unmatched_cpts=[p.cpt_code for p in billed_remaining],
        skipped_cpts=skipped_codes,
        unit_issues=unit_messages,
        rate_failure=rate_failure,
        missing_rates=missing_rates
    )


if __name__ == "__main__":
    # For CLI testing or dev batch run
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run.py <file_name.json>")
    else:
        run_claim_validation(sys.argv[1])
