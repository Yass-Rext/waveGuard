from rules import (
    amount_rule,
    flagged_rule,
    location_rule,
    transaction_type_rule,
    velocity_rule,
    risk_level
)

def apply_rules(df):

    df = amount_rule.apply(df)

    df = flagged_rule.apply(df)

    df = location_rule.apply(df)

    df = transaction_type_rule.apply(df)

    df = velocity_rule.apply(df)

    df = risk_level.apply(df)

    return df