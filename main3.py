import threading
import time
import queue
import csv
import pymysql
import traceback
from whoswho import who

# 子執行緒類別
class Worker(threading.Thread):
	def __init__(self, input_queue, output_queue,id):
		threading.Thread.__init__(self)
		self.input_queue = input_queue
		self.output_queue = output_queue
		self.db = pymysql.connect("127.0.0.1", "wai", "123", "mm", charset='utf8' )
		self.cursor = self.db.cursor()
		self.id = id

	def run(self):
		while self.input_queue.qsize() > 0:
			try:
				ceo_row = self.input_queue.get()
				ceo_year = ceo_row[1]
				ceo_id = ceo_row[2]
				ceo_name = ceo_row[4]
				ceo_company = ceo_row[7]
				ceo_company_name_first_word = (ceo_company.split())[0]

				ceo_name = ceo_name.replace("\'", "\'\'")
				ceo_company = ceo_company.replace("\'", "\'\'")

				#Find the possible company name of the CEO
				sql = 'SELECT cname, namefmac, compustatname, Acronym, year, exec_fullname from Company WHERE (cname = \"'+ ceo_company+'\" OR namefmac = \"'+ ceo_company +'\" OR compustatname = \"'+ ceo_company+'\" OR Acronym = \"'+ ceo_company+'\") AND (year='+ ceo_year+');';
				#print(sql)

				self.cursor.execute(sql)
				results = self.cursor.fetchall()

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
					cname_year2 = cname_year1 - 1
						
					sql = 'SELECT * from Donation WHERE Cycle in ('+str(cname_year1)+','+str(cname_year2)+') AND (Orgname in (\''+cname1+'\',\''+cname2+'\',\''+cname3+'\',\''+cname4+'\') OR (Orgname LIKE \"'+ ceo_company_name_first_word+'%\")) AND Orgname <> \'\''
						 
					self.cursor.execute(sql)
					donation_results = self.cursor.fetchall()

					for donation_result in donation_results:
						donation_donor_name = donation_result[3]
						donation_donor_name = donation_donor_name.replace("\'", "\'\'")
						if(who.match(ceo_name,donation_donor_name)):
							sql = 'SELECT * from Donation WHERE RecipID = \"' + donation_donor_name + '\";'
							self.cursor.execute(sql)
							name_matched_donation_results = self.cursor.fetchall()

							for name_matched_donation_result in name_matched_donation_results:
								output_row = [ceo_id] + list(name_matched_donation_result)
								self.output_queue.append(output_row)
			except Exception:
				traceback.print_exc()
			finally:
				# 處理資料
				print("Worker %d: %s" % (self.id, self.input_queue.qsize()))

		
     


# 建立 5 個子執行緒
def main():

	# 建立佇列
	input_queue = queue.Queue()
	output_queue = queue.Queue()

	# 將資料放入佇列
	with open('Ceo.csv', newline='') as csvfile:


		# 讀取 CSV 檔案內容
		rows = csv.reader(csvfile)
		next(rows, None)

		# 以迴圈輸出每一列
		for row in rows:
			input_queue.put(row)

	thread_num = 10
	threads = []
	for i in range(thread_num):
		threads.append(Worker(input_queue, output_queue, i))

	for i in range(thread_num):
		threads[i].start()

	for i in range(thread_num):
		threads[i].start()
		threads[i].join()

	with open('output.csv', 'w', newline='') as csvfile:
		 # 建立 CSV 檔寫入器
		writer = csv.writer(csvfile)

	writer.writerow(['execid', 'Cycle', 'Date', 'ContribID', 'Contrib', 'RecipID', 'Orgname', 'UltOrg', 'Occupation', 'RealCode', 'Amount', 'State', 'Zip', 'RecipCode', 'Type', 'Gender', 'reccode1', 'reccode2'])

	while(output_queue.qsize() > 0):
		output_row = output_queue.get()
		writer.writerow(output_row)


	
	
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