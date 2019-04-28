import threading
import time
import queue
import csv
import pymysql
import traceback

# 子執行緒類別
class Worker(threading.Thread):
  def __init__(self, queue, num):
    threading.Thread.__init__(self)
    self.queue = queue
    self.num = num

  def run(self):
    while self.queue.qsize() > 0:
      # 取得新的資料
      msg = self.queue.get()

      # 處理資料
      print("Worker %d: %s" % (self.num, msg))
      time.sleep(1)

# 建立 5 個子執行緒
def main():

	# 建立佇列
	my_queue = queue.Queue()

	# 將資料放入佇列
	with open('Ceo.csv', newline='') as csvfile:


		# 讀取 CSV 檔案內容
		rows = csv.reader(csvfile)
		next(rows, None)

		# 以迴圈輸出每一列
		for row in rows:
			my_queue.put(row)

	db = pymysql.connect("192.168.1.58", "wai", "123", "mm", charset='utf8' )
	cursor = db.cursor()

	i=0
	try:
		while not my_queue.empty():
			i+=1
			ceo_row = my_queue.get()
			ceo_year = ceo_row[1]
			ceo_name = ceo_row[4]
			ceo_company = ceo_row[7]

			ceo_name = ceo_name.replace("\'", "\'\'")
			ceo_company = ceo_company.replace("\'", "\'\'")

			sql = 'SELECT cname, namefmac, compustatname, Acronym from Company WHERE (cname = \"'+ ceo_company+'\" OR namefmac = \"'+ ceo_company +'\" OR compustatname = \"'+ ceo_company+'\" OR Acronym = \"'+ ceo_company+'\") AND (year='+ ceo_year+');';
			#print(sql)

			cursor.execute(sql)
			results = cursor.fetchall()

			if(i%5000==0):
				print(i)

			if(len(results) > 0):
				print(ceo_company)
				print(results)
				input()
			"""
			for row in results:
				fname = row[0]
				lname = row[1]
				age = row[2]
				sex = row[3]
				income = row[4]
				print(fname)
			"""
	except Exception:
		traceback.print_exc()
	"""
	threads = []
	for i in range(5):
	  threads.append(Worker(my_queue,i))
	  threads[i].start()

	# 主執行緒繼續執行自己的工作
	# ...

	# 等待所有子執行緒結束
	for i in range(5):
	  threads[i].join()

	print("Done.")
	"""
main()