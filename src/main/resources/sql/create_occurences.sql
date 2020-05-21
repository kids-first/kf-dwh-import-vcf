create view variant_live.occurrences as
select chromosome, start, end, reference, alternate, name, biospecimen_id, genotype.calls, has_alt, is_multi_alellic, old_multi_allelic, hgvsg, variant_class, participant_id, family_id, bucket, study_id, release_id, dbgap_consent_code from variant.occurrences_sd_dypmehhf_re_000001
union
select chromosome, start, end, reference, alternate, name, biospecimen_id, genotype.calls, has_alt, is_multi_alellic, old_multi_allelic, hgvsg, variant_class, participant_id, family_id, bucket, study_id, release_id, dbgap_consent_code from variant.occurrences_sd_9pyzahhe_re_000001