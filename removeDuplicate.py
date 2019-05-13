import queue
import csv
import traceback


def main():

	# 建立佇列
	input_queue = queue.Queue()
	output_queue = queue.Queue()
	donation_set = set()

	# 將資料放入佇列
	with open('output_from main3.csv', newline='') as csvfile:


		# 讀取 CSV 檔案內容
		rows = csv.reader(csvfile)
		next(rows, None)

		# 以迴圈輸出每一列
		for row in rows:
			input_queue.put(row)

	while input_queue.empty() == False:
		row = input_queue.get();
		temp_tuple = tuple(row);
		if(temp_tuple not in donation_set):
			donation_set.add(temp_tuple)
			output_queue.put(temp_tuple)


	print("Start writing remove duplicate output file")

	with open('no duplicate output.csv', 'w', newline='') as csvfile:
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