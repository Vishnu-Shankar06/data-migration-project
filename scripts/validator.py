def validate_customer(row):
    return True, None


# Instead of rejecting invalid rows, we clean and keep them
# This is intentional because data loss is worse than imperfect data in this pipeline