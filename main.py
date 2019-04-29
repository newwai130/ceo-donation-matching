import threading
import time
import queue
import csv
import pymysql
import traceback
from whoswho import who

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

	db = pymysql.connect("127.0.0.1", "wai", "123", "mm", charset='utf8' )
	cursor = db.cursor()

	i=0
	found = 0
	try:
		while not my_queue.empty():
			i+=1
			ceo_row = my_queue.get()
			ceo_year = ceo_row[1]
			ceo_name = ceo_row[4]
			ceo_company = ceo_row[7]

			ceo_name = ceo_name.replace("\'", "\'\'")
			ceo_company = ceo_company.replace("\'", "\'\'")

			sql = 'SELECT cname, namefmac, compustatname, Acronym, year, exec_fullname from Company WHERE (cname = \"'+ ceo_company+'\" OR namefmac = \"'+ ceo_company +'\" OR compustatname = \"'+ ceo_company+'\" OR Acronym = \"'+ ceo_company+'\") AND (year='+ ceo_year+');';
			#print(sql)

			cursor.execute(sql)
			results = cursor.fetchall()

			if(i%1000==0):
				print(i)

			for row in results:
				cname1 = row[0]
				cname2 = row[1]
				cname3 = row[2]
				cname4 = row[3]
				ceo_name = row[5]

				cname1 = cname1.replace("\'", "\'\'")
				cname2 = cname2.replace("\'", "\'\'")
				cname3 = cname3.replace("\'", "\'\'")
				cname4 = cname4.replace("\'", "\'\'")

				cname_year1 = row[4]
				cname_year2 = cname_year1 + 1
				
				sql = 'SELECT * from Donation WHERE Cycle in ('+str(cname_year1)+','+str(cname_year2)+') AND Orgname in (\''+cname1+'\',\''+cname2+'\',\''+cname3+'\',\''+cname4+'\') AND Orgname <> \'\''
				cursor.execute(sql)
				donation_results = cursor.fetchall()
				if(len(donation_results) > 0):
					for donation_result in donation_results:
						donor = donation_result[3]
						if(who.match(ceo_name,donor)):
							#print(ceo_row)
							#print(donation_result)
							found += 1
		print("found: ", found)					
	except Exception:
		print(sql)
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