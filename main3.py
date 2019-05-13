import threading
import time
import queue
import csv
import pymysql
import traceback
from whoswho import who
from time import time

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
		while self.input_queue.empty() == False:
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

				if(len(results) > 0):
					print("Worker %d: in progress matching company,    length: %s" % (self.id, len(results)))

				for row in results:
					cname1 = row[0] if row[0] else "Condition never match"
					cname2 = row[1] if row[1] else "Condition never match"
					cname3 = row[2] if row[2] else "Condition never match"
					cname4 = row[3] if row[3] else "Condition never match"
					ceo_name = row[5] if row[5] else "Condition never match"

					cname1 = cname1.replace("\'", "\'\'")
					cname2 = cname2.replace("\'", "\'\'")
					cname3 = cname3.replace("\'", "\'\'")
					cname4 = cname4.replace("\'", "\'\'")

					cname_year1 = row[4]
					cname_year2 = cname_year1 - 1
						
					sql = 'SELECT Contrib from Donation WHERE Cycle in ('+str(cname_year1)+','+str(cname_year2)+') AND (Orgname in (\''+cname1+'\',\''+cname2+'\',\''+cname3+'\',\''+cname4+'\') OR (Orgname LIKE \"'+ ceo_company_name_first_word+' %\")) AND Orgname <> \'\''
						 
					self.cursor.execute(sql)
					donation_results = self.cursor.fetchall()
					if(len(donation_results) > 0):
						print("Worker %d: in progress finding donation,   job id: %d  CEO:  %d    Donation:  %d   Current: %d" % (self.id, self.input_queue.qsize(), len(results), len(donation_results), self.output_queue.qsize()))
					else:
						print("Worker %d: in progress finding donation,   job id: %d  CEO:  %d    Current: %d" % (self.id, self.input_queue.qsize(),len(results), self.output_queue.qsize()))

					for donation_result in donation_results:
						donation_donor_name = donation_result[0]
						donation_donor_name = donation_donor_name.replace("\'", "\'\'")
						if(who.match(ceo_name,donation_donor_name)):
							sql = 'SELECT * from Donation WHERE Contrib = \"' + donation_donor_name + '\";'
							self.cursor.execute(sql)
							name_matched_donation_results = self.cursor.fetchall()

							if(len(name_matched_donation_results) > 0):
								print("Worker %d: in progress matching name,    length: %s" % (self.id, len(name_matched_donation_results)))


							for name_matched_donation_result in name_matched_donation_results:
								output_row = [ceo_id] + list(name_matched_donation_result)
								self.output_queue.put(output_row)
			except Exception:
				traceback.print_exc()
			finally:
				# 處理資料
				print("Worker %d: %s" % (self.id, self.input_queue.qsize()))
		print("Worker %d  End" % (self.id))


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



	thread_num = 16
	threads = []
	for i in range(thread_num):
		threads.append(Worker(input_queue, output_queue, i))
		threads[i].start()

	for i in range(thread_num):
		threads[i].join()

	print("Start writing output file")

	with open('output.csv', 'w', newline='') as csvfile:
		 # 建立 CSV 檔寫入器
		writer = csv.writer(csvfile)

		writer.writerow(['execid', 'Cycle', 'Date', 'ContribID', 'Contrib', 'RecipID', 'Orgname', 'UltOrg', 'Occupation', 'RealCode', 'Amount', 'State', 'Zip', 'RecipCode', 'Type', 'Gender', 'reccode1', 'reccode2'])

		while(output_queue.empty() == False):
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