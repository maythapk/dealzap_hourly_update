from bs4 import BeautifulSoup
import pandas as pd
import urllib2
import gspread
import luigi
import datetime
from oauth2client.service_account import ServiceAccountCredentials
import os

DATA_DIR_PATH = os.path.join(os.path.dirname(__file__),'data/')

PRODUCT_ACTIVE_QUERY = """
SELECT
  ACT.product_id as product_id ,
  IF(ACT.quantity = 0, FALSE, ACT.product_active) as product_active,
  ACT.quantity as quantity,
  CAT.group_category as group_cat,
  CAT.main_category_name as main_cat,
  CAT.sub_category_name as sub_cat
FROM (
  SELECT product_id, product_active, quantity FROM [dd_recommender_weloveshopping_production.PRODUCT_ACTIVE]
  WHERE product_id IN ('{}')
  ) ACT
LEFT JOIN
  [aggregate.PD_latest_products] PD ON ACT.product_id = PD.product_id
LEFT JOIN
  [Seller_stores.categories] CAT ON PD.category_last_id = CAT.sub_category_id
"""
DEALZAP_URL = "https://portal.weloveshopping.com/dealzapp"
SCOPES = ['https://spreadsheets.google.com/feeds']

class DealzapScraper(luigi.Task):

    now = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR_PATH,"dealzap_scraped_{}.csv".format(self.now.strftime("%Y_%m_%d_%H"))))
    def run(self):
        print "+++++++++ Scraping Now +++++++++++++++"
        soup = BeautifulSoup(urllib2.urlopen(DEALZAP_URL))
        dealzap_content = soup.find('ul', class_='items-list-box')
        dealzap_items = {'product_url':[], 'product_name':[],'product_img_url':[], 'seller_url':[],'seller_name':[], 'discount_price':[], 'usual_price':[]}
        for item in dealzap_content.findAll('li'):
            dealzap_items['product_url'].append(item.findNext('a').get('href'))
            dealzap_items['product_name'].append(item.find(class_='item-detail').find('a').get_text())
            dealzap_items['seller_url'].append(item.find(class_='shop-name').find('a').get('href'))
            dealzap_items['seller_name'].append(item.find(class_='shop-name').find('a').get_text())
            dealzap_items['discount_price'].append(item.find(class_='price').get_text())
            dealzap_items['usual_price'].append(item.find(class_='compare').get_text())
            dealzap_items['product_img_url'].append(item.find(class_='item-img').get('src'))

        dealzap_df = pd.DataFrame(dealzap_items, columns=['product_url', 'product_name','product_img_url','seller_url','seller_name', 'discount_price','usual_price'])
        dealzap_df.head()
        # extract product_id from product_url
        dealzap_df['product_id'] = [ splited[-1:][0] for splited in dealzap_df.product_url.str.split("/")]
        # extract numbers only
        dealzap_df['usual_price'] = dealzap_df.usual_price.str.replace(",","").str.extract("([0-9]+)")
        dealzap_df['discount_price'] = dealzap_df.discount_price.str.replace(",","").str.extract("([0-9]+)")
        with self.output().open('w') as f_out:
            dealzap_df.to_csv(f_out, encoding='utf-8', index=False)

class DealzapStockChecker(luigi.Task):
    now = luigi.Parameter()
    def requires(self):
        return DealzapScraper(self.now)
    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR_PATH,"dealzap_stock_updated_{}.csv".format(self.now.strftime("%Y_%m_%d_%H"))))
    def run(self):
        dealzap_scraped_df = pd.read_csv(self.input().open(), sep=',')
        stock_update = pd.io.gbq.read_gbq(PRODUCT_ACTIVE_QUERY.format("','".join(dealzap_scraped_df.product_id.tolist())), 'weloveshopping-973')
        agg_df = dealzap_scraped_df.merge(stock_update, how='left', on='product_id')
        with self.output().open('w') as f_out:
            agg_df.to_csv(f_out, encoding='utf-8', index=False)


class Export(luigi.Task):
    now = datetime.datetime.now()
    def requires(self):
        return DealzapStockChecker(self.now)
    #def output(self):
    #    return luigi.LocalTarget("dealzap_agg.csv")
    def run(self):
        agg_df = pd.read_csv(self.input().open('r'))
        self._push_to_gd(agg_df)

    def _push_to_gd(self, agg_df):
        print "+++++++++ Pushing to Google Sheet {} +++++++++++++++".format(self.now.strftime("%Y %m %d %H:%M"))
        def numberToLetters(q):
            q = q - 1
            result = ''
            while q >= 0:
                remain = q % 26
                result = chr(remain+65) + result;
                q = q//26 - 1
            return result
        credentials = ServiceAccountCredentials.from_json_keyfile_name('maythapk-ascend-a186f84648e6.json', SCOPES)
        gc = gspread.authorize(credentials)
        sh = gc.open('WLS_dealzap')
        ws = sh.sheet1

        num_rows, num_cols = agg_df.shape

        # clear content
        cell_list = ws.range('A2:' + numberToLetters(num_cols)+str(200+1))
        for cell in cell_list:
            cell.value = ""
        ws.update_cells(cell_list)

        # Update items
        cell_list = ws.range('A2:' + numberToLetters(num_cols)+str(num_rows+1))
        for cell in cell_list:
            val = agg_df.iloc[cell.row-2,cell.col-1]
            if type(val) is str:
                val = val.decode('utf-8')
            elif isinstance(val, (int, long, float, complex)):
                # note that we round all numbers
                val = int(round(val))
            cell.value = val

        ws.update_cells(cell_list)

#class save_to_googlesheet(luigi.Task):
#    def require(self):
#        return dealzap_aggregator()
#
#  def run(self):
#       SCOPES = ['https://spreadsheets.google.com/feeds']
if __name__ == "__main__":
    luigi.run()
