import pandera.pandas as pa
from pandera.pandas import Column, Check, DataFrameSchema

# SDET Core: Define the "Contract" for valid data.
AnalysisSchema = DataFrameSchema({
    "Name": Column(str, checks=[
        Check(lambda x: len(x) > 0, error="Name cannot be empty")
    ]),
    "Salary": Column(int, checks=[
        # MLB Minimum is roughly $740k. Warn if below.
        Check.ge(740000, error="Salary is below MLB minimum"),
        # Soft cap at $100M to catch parsing errors
        Check.le(100000000, error="Salary exceeds realistic max ($100M)")
    ]),
    "WAR": Column(float, checks=[
        # WAR is rarely below -5.0 or above 15.0
        Check.ge(-5.0, error="WAR looks suspiciously low"),
        Check.le(15.0, error="WAR looks suspiciously high")
    ], coerce=True)
})


def validate_analysis_data(df):
    """
    Applies the schema validation to the dataframe.
    Returns the dataframe if valid, raises SchemaError if invalid.
    """
    try:
        return AnalysisSchema.validate(df)
    except pa.errors.SchemaError as e:
        print(f"❌ Data QA Failed: {e}")
        raise e
