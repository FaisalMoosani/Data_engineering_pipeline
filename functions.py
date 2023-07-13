import os
from bs4 import BeautifulSoup
import pandas as pd

def DataIngestion(directory_path):
    # List to store final tables for each file
    final_tables = []

    # Traverse through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory_path, filename)

            try:
                # Load XML file using Beautiful Soup
                with open(file_path, 'r') as file:
                    xml_data = file.read()
                soup = BeautifulSoup(xml_data, 'xml')

                # Extract data from productAvailabilityHeader (Header)
                header_list = []
                header_elements = soup.find_all('productAvailabilityHeader')
                for header_element in header_elements:
                    header_data = {}
                    for header_child in header_element.children:
                        if header_child.name is not None:
                            header_data[header_child.name] = header_child.text
                    header_list.append(header_data)

                # Extract data from brandArticleReports (Main report)
                brand_article_reports_list = []
                brand_article_reports = soup.find_all('brandArticleReport')
                for brand_article_report in brand_article_reports:
                    brand_code = brand_article_report['brandCode']
                    article_reports = brand_article_report.find_all('articleReport')
                    for article_report in article_reports:
                        tb_article_id = article_report.find('tbArticleId').text
                        ean = article_report.find('ean').text
                        article_data = {
                            'brandCode': brand_code,
                            'tbArticleId': tb_article_id,
                            'ean': ean
                        }
                        for article_child in article_report.children:
                            if article_child.name is not None and article_child.name not in ['tbArticleId', 'ean','articleKey']:
                                article_data[article_child.name] = article_child.text
                        brand_article_reports_list.append(article_data)

                # Create productAvailabilityHeader and brandArticleReports tables using pandas DataFrame
                header_table = pd.DataFrame(header_list)
                brand_article_reports_table = pd.DataFrame(brand_article_reports_list)

                # Combine header table and brand article reports table
                combined_table = header_table.merge(brand_article_reports_table, how="cross")

                # Add the combined table to the final tables list
                final_tables.append(combined_table)

            except Exception as e:
                print(f"Error processing file: {file_path}")
                print(f"Error message: {str(e)}")

    # Concatenate all final tables into a single DataFrame
    final_table = pd.concat(final_tables, ignore_index=True)

    return final_table



def PreProcessing(df):
    ########### drop Na ########
    df = df.dropna()

    ####### replace unknown values in brandCode with correct value
    # Find article IDs with 'UNKNOWN' brandCode
    unknown_article_ids = df[df['brandCode'] == 'UNKNOWN']['tbArticleId'].unique()

    # Find similar article IDs with known brandCode
    similar_article_ids = df[df['brandCode'] != 'UNKNOWN']['tbArticleId'].unique()

    # Find article IDs that need imputation
    article_ids_to_impute = set(unknown_article_ids) & set(similar_article_ids)

    # Iterate over the article IDs to impute the brandCode
    for article_id in article_ids_to_impute:
        brand_code = df[df['tbArticleId'] == article_id]['brandCode'].iloc[0]
        df.loc[df['tbArticleId'] == article_id, 'brandCode'] = brand_code

    ######## drop duplicates #######
    df = df.drop_duplicates()
   
   ##### replace tb and tb_ from retailerChannelCode and brandCode
    df.loc[:, 'retailerChannelCode'] = df['retailerChannelCode'].str.replace('tb', '')
    df.loc[:, 'brandCode']  = df['brandCode'].str.replace('tb_', '')

    ##### convert lower case to upper case
    df.loc[:, 'retailerChannelCode'] = df['retailerChannelCode'].str.upper()

    df.loc[:, 'statusSupplementary'] = df['statusSupplementary'].str.replace(' ', '')


    return df

def DataTypeConversion(df):
    df['retailerCode'] =  df['retailerCode'].astype('string')
    df['retailerChannelCode'] =  df['retailerChannelCode'].astype('string')
    df['brandCode'] =  df['brandCode'].astype('string')
    df['tbArticleId'] = df['tbArticleId'].astype('string')
    df['ean'] = df['ean'].astype('string')
    df['accepted'] = df['accepted'].astype('string')
    df['statusCode'] = df['statusCode'].astype('int64')
    df['statusSupplementary'] = df['statusSupplementary'].astype('string')
    
    return df


def CovertArticleToFactandDim(df):
    # Create Dimension Tables
    dim_retailer = df[['retailerCode', 'retailerChannelCode']].drop_duplicates().reset_index(drop=True)
    dim_brand = df[['brandCode']].drop_duplicates().reset_index(drop=True)
    dim_status = df[['statusCode', 'statusSupplementary']].drop_duplicates().reset_index(drop=True)
    dim_accepted = df[['accepted']].drop_duplicates().reset_index(drop=True)
        
        
    # Add Surrogate Keys to Dimension Tables
    dim_retailer['retailerKey'] = dim_retailer.index + 1
    dim_brand['brandKey'] = dim_brand.index + 1
    dim_accepted['acceptanceKey'] = dim_accepted.index + 1

    # Create Fact Table with Foreign Keys
    fact_article = df.merge(dim_retailer, on=['retailerCode', 'retailerChannelCode'], how='left')
    fact_article = fact_article.merge(dim_brand, on=['brandCode'], how='left')
    fact_article = fact_article.merge(dim_status, on=['statusCode', 'statusSupplementary'], how='left')
    fact_article = fact_article.merge(dim_accepted, on=['accepted'], how='left')

    # Rearrange Columns in Fact Table
    fact_article = fact_article[['retailerKey', 'brandKey','statusCode', 'acceptanceKey','tbArticleId', 'ean']]
    return dim_retailer, dim_brand, dim_status,dim_accepted, fact_article
