from server.modules.web_scraper.web_scraper import WebScraper
import re
import pandas as pd
import json
import logging
logging.basicConfig(level= logging.INFO)

class ScreenerScraper(WebScraper):
    
    def __init__(self):
        super(WebScraper, self).__init__()
        self.site_url = "https://screener.in/"
        self.sections_to_scrape = {
            "main": [{"id":"top"}, {"id":"peers"}, {"id":"quarters"}, {"id":"profit-loss"}, {"id":"balance-sheet"}, 
            {"id":"cash-flow"}, {"id":"ratios"}, {"id":"shareholding"},
            {"id":"documents"}]
        }
        self.number_regex = re.compile(r"-*\d+\.*\d*")


    def filter_floats(self, string_number):
        if type(string_number) == dict or not string_number:
            return string_number 
        return float("".join(re.findall(self.number_regex,string_number)))
    

    def format_table_data(self, table_df):

        all_columns = table_df.columns.tolist()[1:]
        for col in all_columns:
            table_df[col] = table_df[col].apply(self.filter_floats)
        return table_df
    
    def get_company_profile(self, profile_tag):
        
        profile_info_tag = profile_tag['tag_data'][0]
        return {}
    

    
    def get_company_links(self, link_tag):

        all_links = [] 
        for link in link_tag['tag_data']:
            url = link.get("tag_attributes",{}).get("href","")
            span_tag = [x for x in link['tag_data'] if x['tag_name'] == "span"]
            link_text = ""
            if span_tag:
                link_text = span_tag[0]['tag_data'][0]
            link_type = ""
            if "nse" in url.lower():
                link_type = "nse"
            elif "bse" in url.lower():
                link_type = "bse"
            else:
                link_type = "company"
            all_links.append({"link_text":link_text, "link":url, "link_type":link_type})
        return all_links

    def get_company_ratios(self, ratio_tag):
        final_ratios = {}
    
        ratio_ul_tag = [x for x in ratio_tag['tag_data'] if x['tag_name'] == 'ul'][0]
        for ratio in ratio_ul_tag['tag_data']:
            ratio_name = ratio['tag_data'][0]['tag_data'][0]
            ratio_values = ratio['tag_data'][1]
            final_str = ""
            for val in ratio_values['tag_data']:
                if type(val) == str:
                    final_str += val
                elif type(val) == dict:
                    final_str += val['tag_data'][0]
        
            ratio_value = final_str
            ratio_values = re.findall("\d+\.*\d*", ratio_value) 
            if ratio_name == "High / Low":
                final_ratios["High"] = float(ratio_values[0])
                final_ratios["Low"] = float(ratio_values[1])
            else:
                final_ratios[ratio_name] = float("".join(ratio_values))
        return final_ratios

    def get_top_section(self,top_tag):
        company_info = {}
 
        for tag in top_tag['tag_data']:
        
            if "company-links" in tag.get("tag_attributes",{}).get("class",[]):
                company_info['company_links'] = self.get_company_links(tag)
            
            elif "company-info" in tag.get("tag_attributes",{}).get("class",[]):
                company_info['company_profile'] = self.get_company_profile(tag['tag_data'][0])
                company_info['company_ratios'] = self.get_company_ratios(tag['tag_data'][1])
        
        return company_info
    

    def extract_table(self, table_dict):
        all_table_rows_to_extract = []
        
        for section in table_dict['tag_data']:
            for row in section['tag_data']:
                all_table_rows_to_extract.append(row)
        all_rows = []
        for row in all_table_rows_to_extract:
            temp_row = []
            for cell in row['tag_data']:
                if cell['tag_data']:
                    data = cell['tag_data'][0]
                    if type(data) == dict:
                        temp_row.append(data['tag_data'][0])
                    else:
                        temp_row.append(cell['tag_data'][0])
                else:
                    temp_row.append("")
            all_rows.append(temp_row)
        table_df = pd.DataFrame(all_rows[1:], columns = all_rows[0])
        return table_df
    
    async def get_company_peers(self, warehouse_id):
        url = f"https://www.screener.in/api/company/{warehouse_id}/peers/"
        resp = await self.fetch_url_page(url)
        table_data = resp['parent'][0]
        table_df = self.extract_table(table_data)
        all_names = table_df['Name'].tolist()
        all_peers = []
        for peer in all_names:
            if type(peer) != str:
                stock_screener_url = f"https://www.screener.in/" + peer['tag_attributes']['href']
                stock_name = peer['tag_data'][0]
                all_peers.append({"stock_screener_url": stock_screener_url, "stock_name":stock_name})
        return all_peers
    

    def get_quarterly_results(self, quarters_tag):
        
        for tag in quarters_tag['tag_data']:
            if "data-result-table"  in tag.get("tag_attributes",{}):
                table = tag['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("Quarterly results generated")
                return table_df.to_dict(orient = "records")
            

    def get_profil_loss_results(self, profit_loss):
        
        for tag in profit_loss['tag_data']:
            if "data-result-table"  in tag.get("tag_attributes",{}):
                table = tag['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("profite loss results generated")
                return table_df.to_dict(orient = "records")
            

    def get_balance_sheet(self, result_tab):
        
        for tag in result_tab['tag_data']:
            if "data-result-table"  in tag.get("tag_attributes",{}):
                table = tag['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("balance sheet results generated")
                return table_df.to_dict(orient = "records")
            

    def get_cash_flow_results(self, result_tab):
        
        for tag in result_tab['tag_data']:
            if "data-result-table"  in tag.get("tag_attributes",{}):
                table = tag['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("cash flow results generated")
                print(table_df)
                return table_df.to_dict(orient = "records")
            
    def get_ratios_table(self, result_tab):
        
        for tag in result_tab['tag_data']:
            if "data-result-table"  in tag.get("tag_attributes",{}):
                table = tag['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("ratio results generated")
                print(table_df)
                return table_df.to_dict(orient = "records")
    
    def get_shareholding_table(self, result_tab):
        shp = {}
        for tag in result_tab['tag_data']:
            if tag.get("tag_attributes",{}).get("id", "") == "quarterly-shp" :
                table = tag['tag_data'][0]['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("Yearly shareholding results generated")
                shp['quarterly_shareholding']  = table_df.to_dict(orient = "records")
            elif tag.get("tag_attributes",{}) == "yearly-shp":
                table = tag['tag_data'][0]['tag_data'][0]
                table_df = self.extract_table(table)
                table_df = self.format_table_data(table_df)
                logging.info("Quarterly shareholding results generated")
                shp['quarterly_shareholding']  = table_df.to_dict(orient = "records")
        return shp
    
    async def fetch_stock_data(self, stock_code):

        new_url = f"{self.site_url}company/{stock_code}/consolidated/"
        htmldata_in_dict = await self.fetch_url_page(new_url)
        body_data = htmldata_in_dict.get("parent",[])[1]['tag_data']
        body_data = [x for x in body_data if x]
        
        main_data= body_data[2]['tag_data']
        main_data= [x for x in main_data if x]
        all_company_data = {}
        for tag in main_data:
           
            if "data-company-id" in tag.get("tag_attributes",{}):
                all_company_data['data_company_id'] = tag.get("tag_attributes")['data-company-id']
                all_company_data['data_warehouse_id'] = tag.get("tag_attributes")['data-warehouse-id']
            elif tag.get("tag_attributes","").get("id", "") == 'top':
                all_company_data['company_ratio_and_meta'] = self.get_top_section(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'quarters':
                all_company_data['quarterly_results'] = self.get_quarterly_results(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'profit-loss':
                all_company_data['profit_loss'] = self.get_profil_loss_results(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'balance-sheet':
                all_company_data['balance_sheet'] = self.get_balance_sheet(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'cash-flow':
                all_company_data['cash_flow'] = self.get_cash_flow_results(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'ratios':
                all_company_data['ratios'] = self.get_ratios_table(tag)
            elif tag.get("tag_attributes","").get("id", "") == 'shareholding':
                all_company_data['shareholding'] = self.get_shareholding_table(tag)
            
        
        all_company_data['peers'] = await self.get_company_peers(all_company_data['data_warehouse_id'])
        with open("sample.json", "w") as fp:
            fp.write(json.dumps(all_company_data))
        return all_company_data
            
       

        