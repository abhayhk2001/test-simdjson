import pandas as pd

student_dict1 = {'Name': ['Joe', 'Nat', 'Harry'],
                 'Age': [20, 21, 19], 'Marks': [85.10, 77.80, 91.54]}
student_dict2 = {'Name': ['Joey', 'Nate', 'Abhay'],
                 'Age-Limit': [20, 21, 19], 'Marks': [85.10, 77.80, 91.54]}
student_dict3 = {'Name': ['Joes', 'Nata', 'krishna'],
                 'modin': [20, 21, 19], 'Hi': [85.10, 77.80, 91.54]}

# create DataFrame from dict
student_df1 = pd.DataFrame(student_dict1)
student_df2 = pd.DataFrame(student_dict2)
student_df3 = pd.DataFrame(student_dict3)

# set index using column
student_df1 = student_df1.set_index('Name')
student_df2 = student_df2.set_index('Name')
student_df3 = student_df3.set_index('Name')

df1 = pd.merge(student_df1, student_df2, how='outer',
               left_index=True, right_index=True)
df1 = pd.merge(df1, student_df3, how='outer',
               left_index=True, right_index=True)
print(df1)

df2 = pd.concat([student_df1, student_df2, student_df3], axis=1)
print(df2)
