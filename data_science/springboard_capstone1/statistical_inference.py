import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt
from numpy.random import seed

# Defining helper functions that perform frequently used tasks, such as
# returning a cw/ccw portion of a df or filtering out a specific band.
def get_cw(df):
    """
    Returns the cw entries of a given dataframe.
    """
    return df[df['rotation'] == 'cw']

def get_ccw(df):
    """
    Returns the ccw entries of a given dataframe.
    """
    return df[df['rotation'] == 'ccw']

def get_band(df, band):
    """
    Returns data from a specific band of a given dataframe.
    """
    return df.filter(regex='_%s' % band)


# Loading all of the individual dataframes
df_list = []
coordinates = pd.read_csv('data/coordinates.csv', index_col=0)
df_list.append(coordinates)
devaucouleurs = pd.read_csv('data/devaucouleurs.csv', index_col=0)
df_list.append(devaucouleurs)
exponential = pd.read_csv('data/exponential.csv', index_col=0)
df_list.append(exponential)
extinction = pd.read_csv('data/extinction.csv', index_col=0)
df_list.append(extinction)
fiber = pd.read_csv('data/fiber.csv', index_col=0)
df_list.append(fiber)
flags = pd.read_csv('data/flags.csv', index_col=0)
df_list.append(flags)
isophotal = pd.read_csv('data/isophotal.csv', index_col=0)
df_list.append(isophotal)
m = pd.read_csv('data/m.csv', index_col=0)
df_list.append(m)
model = pd.read_csv('data/model.csv', index_col=0)
df_list.append(model)
object_info = pd.read_csv('data/object_info.csv', index_col=0)
df_list.append(object_info)
petro = pd.read_csv('data/petro.csv', index_col=0)
df_list.append(petro)
position = pd.read_csv('data/position.csv', index_col=0)
df_list.append(position)
prof = pd.read_csv('data/prof.csv', index_col=0)
df_list.append(prof)
psf = pd.read_csv('data/psf.csv', index_col=0)
df_list.append(psf)
signal = pd.read_csv('data/signal.csv', index_col=0)
df_list.append(signal)
sky = pd.read_csv('data/sky.csv', index_col=0)
df_list.append(sky)
stokes = pd.read_csv('data/stokes.csv', index_col=0)
df_list.append(stokes)
target = pd.read_csv('data/target.csv', index_col=0)
df_list.append(target)
texture = pd.read_csv('data/texture.csv', index_col=0)
df_list.append(texture)
types = pd.read_csv('data/types.csv', index_col=0)
df_list.append(types)



# Iterates over columns of a given df and selects columns where a t-test between
# cw and ccw entries (in any band) produce a p-value of p < 0.05. This acts as a
# crude way to perform feature selection.

significant_cols = []
#bands = ['u', 'g', 'r', 'i', 'z']
for df in df_list:
    for col in df.columns[1:]:
        cw_variable = get_cw(df)[col].dropna()
        ccw_variable = get_ccw(df)[col].dropna()
        cw_mean_estimate = np.mean(cw_variable)
        cw_std_estimate = np.std(cw_variable, ddof=1)
        ccw_mean_estimate = np.mean(ccw_variable)
        ccw_std_estimate = np.std(ccw_variable, ddof=1)
        cw_crit_val = stats.t.ppf(0.05, loc=cw_mean_estimate, scale=cw_std_estimate, df=13439)
        ccw_crit_val = stats.t.ppf(0.05, loc=ccw_mean_estimate, scale=ccw_std_estimate, df=13439)
        cw_margin = abs(cw_mean_estimate - cw_crit_val)
        ccw_margin = abs(ccw_mean_estimate - ccw_crit_val)

        t_val, p_val = stats.ttest_ind(cw_variable, ccw_variable, equal_var=False)
        if p_val <= 0.05:
            print('\nname:', col, 't:', t_val, 'p:', p_val)
            print('cw mean:', cw_mean_estimate, 'cw low:', cw_mean_estimate - cw_margin, 
                        'cw high:', cw_mean_estimate + cw_margin)
            print('ccw mean:', ccw_mean_estimate, 'ccw low:', ccw_mean_estimate - ccw_margin, 
                        'ccw high:', ccw_mean_estimate + ccw_margin)
            significant_cols.append(col)
        

#print(significant_cols)
'''
plt.plot(get_cw(exponential)['expPhi_u'], get_cw(devaucouleurs)['deVPhi_u'], color='green', linestyle='none', marker='.', alpha=0.5, markersize=0.5)
plt.plot(get_ccw(exponential)['expPhi_u'], get_ccw(devaucouleurs)['deVPhi_u'], color='yellow', linestyle='none', marker='.', alpha=0.25, markersize=0.5)

plt.show()
'''

#mean_diff = np.mean(get_cw(stokes)['u_u']) - np.mean(get_ccw(stokes)['u_u'])

plt.subplot(211)
plt.hist(get_cw(petro)['petroMag_u'], alpha=0.5)
plt.axvline(np.mean(get_cw(petro)['petroMag_u']))

plt.subplot(212)
plt.hist(get_ccw(petro)['petroMag_u'], alpha=0.5)
plt.axvline(np.mean(get_ccw(petro)['petroMag_u']))

plt.show()

r_1, p_1 = stats.pearsonr(get_cw(exponential)['expRad_z'], get_cw(exponential)['expMag_z'])
r_2, p_2 = stats.pearsonr(get_ccw(exponential)['expRad_z'], get_ccw(exponential)['expMag_z'])

print(r_1, p_1)
print(r_2, p_2)