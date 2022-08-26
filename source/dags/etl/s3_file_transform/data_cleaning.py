import pandas as pd


def data_cleaning(input_file, output_file, job_title_filter=None):

    df = pd.read_csv(input_file)

    if job_title_filter:
        df = df[(df["job_title"].str.contains(job_title_filter))]

    df["company_size"] = df["company_size"].replace(
        ["S", "M", "L"], ["SMALL", "MEDIUM", "LARGE"]
    )

    df["experience_level"] = df["experience_level"].replace(
        ["EN", "MI", "SE", 'EX'], ["ENTRY-LEVEL", "MID-LEVEL", "SENIOR-LEVEL", "EXECUTIVE-LEVEL"]
    )

    df["employment_type"] = df["employment_type"].replace(
        ["PT", "FT", "CT"], ["PART-TIME", "FULL-TIME", "FREELANCE"]
    )

    df = df.drop(df.columns[0], axis=1)

    df.to_parquet(output_file, index=False)
