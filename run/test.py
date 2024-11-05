import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to fetch data from World Bank
def fetch_data():
    # URLs for World Bank data
    gdp_growth_url = 'http://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=excel'
    gini_url = 'http://api.worldbank.org/v2/en/indicator/SI.POV.GINI?downloadformat=excel'
    
    # Fetching and reading data
    gdp_growth_data = pd.read_excel(gdp_growth_url, sheet_name='Data', skiprows=3)
    gini_data = pd.read_excel(gini_url, sheet_name='Data', skiprows=3)
    
    # Melting the data to have one year per row
    gdp_growth_data_melted = gdp_growth_data.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='GDP Growth')
    gini_data_melted = gini_data.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='Gini Coefficient')
    
    # Converting 'Year' from string to integer
    gdp_growth_data_melted['Year'] = gdp_growth_data_melted['Year'].astype(int)
    gini_data_melted['Year'] = gini_data_melted['Year'].astype(int)
    
    # Merging datasets on common columns
    merged_data = pd.merge(gdp_growth_data_melted, gini_data_melted, on=['Country Name', 'Country Code', 'Year'])
    
    # Dropping rows with missing values
    merged_data.dropna(inplace=True)
    
    return merged_data

# Fetch data
data = fetch_data()

# Plotting the correlation for all years
plt.figure(figsize=(10, 6))
sns.regplot(x='Gini Coefficient', y='GDP Growth', data=data, ci=None, scatter_kws={'s': 10})
plt.title('Correlation between Income Inequality (Gini Coefficient) and GDP Growth (All Years)')
plt.xlabel('Gini Coefficient')
plt.ylabel('GDP Growth (%)')

plt.savefig('correlation_plot.png') 
