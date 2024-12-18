import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

def subset_df(df, group):
    '''
    Method for creating a new dataframe for a specified group. 
    Preserves the 'rotation' column from the original dataframe.
    '''
    new_df = df[['rotation', group]].droplevel(0, axis=1)
    new_df.rename(columns={new_df.columns[0]:'rotation'}, inplace=True)
    return new_df


URL = 'http://vfacstaff.ltu.edu/lshamir/data/assym/p_all_full.csv'
df = pd.read_csv(URL)
#FILE_LOC = 'data/data.csv
#df = pd.read_csv(FILE_LOC, index_col=0)

#Dataset has 455 columns, so we need to group them into categories to make our analysis manageable.
col_names = pd.read_csv('data/col_names.csv', names=['category', 'col_name'])
col_groups = col_names.groupby('category')['col_name'].apply(list).to_dict()

#Constructing a new dataframe with multi-level columns corresponding to the categories specified above.
data_df = pd.concat([df[col_groups[key]] for key in col_groups.keys()], 
            axis=1, keys=col_groups.keys())
data_df.insert(0, 'rotation', df['rotation'])

#Creating individual dataframes for each category to perform preliminary analysis.
coordinates = subset_df(data_df, 'coordinates')
devaucouleurs = subset_df(data_df, 'devaucouleurs')
exponential = subset_df(data_df, 'exponential')
extinction = subset_df(data_df, 'extinction')
fiber = subset_df(data_df, 'fiber')
flags = subset_df(data_df, 'flags')
isophotal = subset_df(data_df, 'isophotal')
m = subset_df(data_df, 'm')
model = subset_df(data_df, 'model')
object_info = subset_df(data_df, 'object_info')
petro = subset_df(data_df, 'petro')
position = subset_df(data_df, 'position')
prof = subset_df(data_df, 'prof')
psf = subset_df(data_df, 'psf')
signal = subset_df(data_df, 'signal')
sky = subset_df(data_df, 'sky')
stokes = subset_df(data_df, 'stokes')
target = subset_df(data_df, 'target')
texture = subset_df(data_df, 'texture')
types = subset_df(data_df, 'types')

#Checking to make sure the only 'object' data types in the dataframe are in the 'rotation' column.
assert data_df.select_dtypes(['object']).equals(data_df[['rotation']])

#Checking to make sure there are no None/nan/empty string values in dataframe, and that
#none of the columns have a data type of 'object' (except the 'rotation' column).
for category, column in list(data_df):
    assert data_df[data_df.loc[:, (category, column)] == None].empty
    assert data_df[data_df.loc[:, (category, column)] == np.nan].empty
    assert data_df[data_df.loc[:, (category, column)] == ''].empty
    if category != 'rotation':
        assert data_df.loc[:, (category, column)].dtype != 'object'
    
#Searching for outliers and suspicious patterns. 
 
#First, checking the Stokes Q value across U & Z bands.
plt.subplot(221)
sns.boxplot(x='variable', y='value', hue='rotation', 
           data=pd.melt(stokes[['rotation', 'q_u', 'q_z']], id_vars=['rotation']))

plt.title('Stokes Q Parameter (U & Z)')
plt.xlabel('Band (U or Z)')
plt.ylabel('Polarization')
plt.text(x=-0.01, y=-8.2, s='outlier', fontsize=9)
plt.text(x=-0.01, y=7.5, s='outlier', fontsize=9)
plt.tick_params(labelsize=9)
plt.tight_layout()
plt.legend().remove()

#Noticed two outliers. Plotting another column in the Stokes dataframe.
plt.subplot(222)
sns.boxplot(x='variable', y='value', hue='rotation',
            data=pd.melt(stokes[['rotation', 'qErr_u']], id_vars='rotation'))
plt.title('Error in Stokes Q Parameter (U band)')
plt.xlabel('Stokes Q Error')
plt.ylabel('Polarization')
plt.legend(loc='center left')
plt.tight_layout()

#Noticed values of -9999.00. Plotting more columns in other dataframes.
plt.subplot(223)
sns.boxplot(x='variable', y='value', hue='rotation',
            data=pd.melt(petro[['rotation', 'petroR50_u']], id_vars='rotation'))
plt.title('Petrosian Radius (U band)')
plt.xlabel('Petrosian Radius at 50% Flux')
plt.ylabel('Radius')
plt.tight_layout()
plt.legend().remove()

plt.subplot(224)
sns.boxplot(x='variable', y='value', hue='rotation',
            data=pd.melt(isophotal[['rotation', 'isoPhi_u']], id_vars='rotation'))
plt.title('Isophotal Tilt Angle (U band)')
plt.xlabel('Isophotal Phi')
plt.ylabel('Angle')
plt.tight_layout()
plt.legend().remove()

plt.show()

#As it turns out, this dataset uses value of -9999.00 to denote missing entries.
#Replacing all instances of -9999.00 with NaN.
data_df.replace(-9999.00, np.nan, inplace=True)

#Updating individual dataframes.
coordinates = subset_df(data_df, 'coordinates')
devaucouleurs = subset_df(data_df, 'devaucouleurs')
exponential = subset_df(data_df, 'exponential')
extinction = subset_df(data_df, 'extinction')
fiber = subset_df(data_df, 'fiber')
flags = subset_df(data_df, 'flags')
isophotal = subset_df(data_df, 'isophotal')
m = subset_df(data_df, 'm')
model = subset_df(data_df, 'model')
object_info = subset_df(data_df, 'object_info')
petro = subset_df(data_df, 'petro')
position = subset_df(data_df, 'position')
prof = subset_df(data_df, 'prof')
psf = subset_df(data_df, 'psf')
signal = subset_df(data_df, 'signal')
sky = subset_df(data_df, 'sky')
stokes = subset_df(data_df, 'stokes')
target = subset_df(data_df, 'target')
texture = subset_df(data_df, 'texture')
types = subset_df(data_df, 'types')

#Our data is now clean and ready for analysis! We can save these as csv files locally
#so we do not have to run this procedure every time we need to perform analysis.
coordinates.to_csv('data/coordinates.csv')
devaucouleurs.to_csv('data/devaucouleurs.csv')
exponential.to_csv('data/exponential.csv')
extinction.to_csv('data/extinction.csv')
fiber.to_csv('data/fiber.csv')
flags.to_csv('data/flags.csv')
isophotal.to_csv('data/isophotal.csv')
m.to_csv('data/m.csv')
model.to_csv('data/model.csv')
object_info.to_csv('data/object_info.csv')
petro.to_csv('data/petro.csv')
position.to_csv('data/position.csv')
prof.to_csv('data/prof.csv')
psf.to_csv('data/psf.csv')
signal.to_csv('data/signal.csv')
sky.to_csv('data/sky.csv')
stokes.to_csv('data/stokes.csv')
target.to_csv('data/target.csv')
texture.to_csv('data/texture.csv')
types.to_csv('data/types.csv')









