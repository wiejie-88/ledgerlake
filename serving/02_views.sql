-- Masked customer view for analysts: initial of the name only, no contact details or date of birth.
-- The view runs with its owner's privileges (security_invoker is off), so analysts can read it without any
-- privilege on serving.customer_profile.

CREATE OR REPLACE VIEW serving.v_customer_profile_masked AS
SELECT
    customer_id,
    left(full_name, 1) || '***' AS name_masked,
    age_band,
    state,
    income_band
FROM serving.customer_profile;

COMMENT ON VIEW serving.v_customer_profile_masked IS
    'Masked customer profile: name initial + ***, age band, state, income band. Analyst-readable.';
COMMENT ON TABLE serving.customer_profile IS 'Contains personal data. Engineer role only.';
