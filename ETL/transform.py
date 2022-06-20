import pandas as pd
import ast
import re


def run():
    nofluffjobs_df = pd.read_csv("staging.csv", index_col=0)

    # nofluffjobs_df_old = nofluffjobs_df
    # nofluffjobs_df_old['skills'] = nofluffjobs_df_old['skills'].str.cat(sep=',')

    nofluffjobs_df[["min_salary", "max_salary"]] = nofluffjobs_df['salary'].str.split('-', expand=True)

    nofluffjobs_df['min_salary'] = nofluffjobs_df['min_salary'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
    nofluffjobs_df['max_salary'] = nofluffjobs_df['max_salary'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)

    for i in range(len(nofluffjobs_df)):
        if pd.isna(nofluffjobs_df['max_salary'][i]):
            nofluffjobs_df.loc[i, 'max_salary'] = nofluffjobs_df['min_salary'][i]
        if pd.isna(nofluffjobs_df['min_salary'][i]):
            nofluffjobs_df.loc[i, 'min_salary'] = '0'
            nofluffjobs_df.loc[i, 'max_salary'] = '0'
    del i

    nofluffjobs_df = nofluffjobs_df.drop(columns='salary')

    nofluffjobs_df['level'] = nofluffjobs_df['level'].str.split(', ', expand=False)
    nofluffjobs_df['category'] = nofluffjobs_df['category'].str.split(', ', expand=False)

    def cleansing_str_list(column: str):
        for a in range(len(nofluffjobs_df)):
            if type(nofluffjobs_df[column][a]) is str:
                nofluffjobs_df[column][a] = ast.literal_eval(nofluffjobs_df[column][a])
            for b in range(len(nofluffjobs_df[column][a])):
                nofluffjobs_df[column][a][b] = nofluffjobs_df[column][a][b].strip().casefold()
            for b in range(len(nofluffjobs_df[column][a])):
                if '&' in nofluffjobs_df[column][a][b]:
                    concatenated = ','.join(nofluffjobs_df[column][a])
                    nofluffjobs_df[column][a] = re.split(',| & ', concatenated)
                if nofluffjobs_df[column][a][b].lower() == 'inne':
                    nofluffjobs_df[column][a][b] = 'other'

    cleansing_str_list('level')
    cleansing_str_list('category')
    cleansing_str_list('skills')

    def to_boolean_df(item_lists, unique_items):
        bool_dict = {}

        for item in unique_items:
            bool_dict[item] = item_lists.apply(lambda x: item in x)

        return pd.DataFrame(bool_dict)

    # Unique items in column of lists
    def unique(series):
        return set(pd.Series([x for _list in series for x in _list]))

    # unique_levels = unique(nofluffjobs_df['level'])
    # unique_categories = unique(nofluffjobs_df['category'])
    unique_skills = unique(nofluffjobs_df['skills'])
    unique_levels = ['trainee', 'junior', 'mid', 'senior', 'expert']

    # unique_skillsets = pd.Series([list(x) for x in set(tuple(x) for x in nofluffjobs_df['skills'])])
    # unique_catsets = pd.Series([list(x) for x in set(tuple(x) for x in nofluffjobs_df['category'])])
    # unique_levelsets = pd.Series([list(x) for x in set(tuple(x) for x in nofluffjobs_df['level'])])

    boolean_levels_df = to_boolean_df(nofluffjobs_df['level'], unique_levels)
    boolean_skills_df = to_boolean_df(nofluffjobs_df['skills'], unique_skills)
    # boolean_categories_df = to_boolean_df(nofluffjobs_df['category'], unique_categories)

    nofluffjobs_df = pd.concat([nofluffjobs_df, boolean_levels_df], axis=1)
    nofluffjobs_df = pd.concat([nofluffjobs_df, boolean_skills_df], axis=1)
    # nofluffjobs_df = pd.concat([nofluffjobs_df, boolean_categories_df], axis=1)

    # del nofluffjobs_df['level']

    nofluffjobs_df['min_salary'] = nofluffjobs_df['min_salary'].astype(int)
    nofluffjobs_df['max_salary'] = nofluffjobs_df['max_salary'].astype(int)

    nofluffjobs_df['skills'] = nofluffjobs_df['skills'].astype(str)

    nofluffjobs_df.to_csv('staging.csv')

    # first query
    # nofluffjobs_df[['min_salary', 'skills']].groupby("skills").mean().sort_values("min_salary", ascending=False).head(10)

    # Creating dimensions for unique skillsets and unique categoriesets would be cool


if __name__ == '__main__':
    run()
