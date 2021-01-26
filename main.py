import re
import sys
from operator import itemgetter

tables = {}
table_content = {}
table_columns = {}

# Carries out a cartesian product between two tables
def join_tables(t_attr, curr_columns):
	new_arr = []
	curr_arr = []
	for w in list(table_columns[t_attr[0]].keys()):
		curr_columns.append(t_attr[0] + "." + w)
	curr_arr = table_content[t_attr[0]]
	
	if len(t_attr) == 1:
		new_arr = curr_arr
	
	for i in range(1, len(t_attr)):
		for j in list(table_columns[t_attr[i]].keys()):
			curr_columns.append(t_attr[i] + "." + j)
		new_arr = []
		
		for z in curr_arr:
			for g in table_content[t_attr[i]]:
				temp_arr = []
				for h in z:
					temp_arr.append(h)
				for h in g:
					temp_arr.append(h)
				new_arr.append(temp_arr)
	
	return new_arr

# The initial parser to read the data files and store them into tables
def getdata():
	f = open("metadata.txt", mode = "r")
	w = f.readlines()
	flag = 0
	for temp in w:
		temp = temp.split('\n')[0]
		if temp == "<begin_table>":
			cnt = 0
			flag = 1
			temp_dict = {}
			continue
		if temp == "<end_table>":
			table_columns[curr] = temp_dict
			continue
		if flag == 1:
			curr = temp
			flag = 0
			continue
		if flag == 0:
			temp_dict[temp] = cnt
			cnt += 1
			if curr not in list(tables.keys()):
				tables[curr] = [temp]
			else:
				tables[curr].append(temp)
	f.close()

	for table in list(tables.keys()):
		f = open(table + ".csv", mode = "r")
		w = f.readlines()
		arr = []
		for temp in w:
			temp = temp.split(",")
			temp[-1] = temp[-1].split("\n")[0]
			z = []
			for x in temp:
				z.append(int(x))
			arr.append(z)
		table_content[table] = arr

	# print(tables) # name of table and their column names
	# print(table_content) # rows of the table
	# print(table_columns) # {'table1': {'A': 0, 'B': 1, 'C': 2}, 'table2': {'B': 0, 'D': 1}}


def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		pass 
	try:
		import unicodedata
		unicodedata.numeric(s)
		return True
	except (TypeError, ValueError):
		pass 
	return False

def check(a, b, c):
	if c == 0 and a < b:
		return 0
	if c == 1 and a > b:
		return 0
	if c == 2 and a >= b:
		return 0
	if c == 3 and a <= b:
		return 0
	if c == 4 and a != b:
		return 0
	return 1

def get_attr(tab_attr, attr):
	t_attr = []
	for x in tab_attr:
		if " " in x:
			x = re.split(" ", x)
			if len(x[1]) == 0:
				x = x[0]
			else:
				if len(x) > 3:
					print("Error : Invalid Column")
					return False, False
				x = x[1]
		t_attr.append(x)
	if len(t_attr) == 0:
		print("Error: Insufficient number of tables")
		return False, False
	for w in t_attr:
		if w not in list(tables.keys()):
			print("Error : Invalid Table")
			return False, False
	fattr = []
	for x in attr:
		if len(x) > 0:
			fattr.append(x)
	attr = []
	for x in fattr:
		x = str(x)
		y = re.split(",", x)
		for q in y:
			if len(q) > 0:
				attr.append(q)
	return t_attr, attr

def check_table(t, t_attr):
	if t not in t_attr:
		return False
	return True

def check_column(t, c):
	if c not in list(table_columns[t].keys()):
		return False
	return True

# checks if that column is present in the table
def check_attr(a, t_attr):
	if "." in a:
		x = re.split("\.", a)
		if check_table(x[0], t_attr) == False:
			print("Error : Invalid Table")
			return False, False
		if check_column(x[0], x[1]) == False:
			print("Error : Invalid Column")
			return False, False
		return True, a
	else:
		flag = 0
		res = ""
		for w in t_attr:
			if a in list(table_columns[w].keys()):
				flag += 1
				res = w
		if flag != 1:
			if flag > 1:
				print("Error : Ambiguous column")
			else:
				print("Error : Invalid Column")
			return False, False
		return True, res + "." + a

def get_no(curr_columns, col):
	for i in range(len(curr_columns)):
		if curr_columns[i] == col:
			return i

def handle_join(fj_arr, curr_columns, final_arr, it):
	rem = set()
	c_arr = []
	nf_arr = []
	new_rem = []
	for i in range(it):
		if fj_arr[i][2] == 4 and is_number(fj_arr[i][0]) == False and is_number(fj_arr[i][1]) == False:
			rem.add(get_no(curr_columns, fj_arr[i][0]))
			rem.add(get_no(curr_columns, fj_arr[i][1]))
	rem = list(rem)
	for i in range(1, len(rem)):
		new_rem.append(rem[i])
	for i in range(len(curr_columns)):
		if i not in new_rem:
			c_arr.append(curr_columns[i])
	for x in final_arr:
		ty = []
		for i in range(len(x)):
			if i not in new_rem:
				ty.append(x[i])
		nf_arr.append(ty)
	final_arr = nf_arr
	curr_columns = c_arr
	return curr_columns, final_arr

def print_result(req_arr, final_arr):
	if len(req_arr[0]) == 2:
		z_arr = []
		for x in req_arr:
			if x[1] == 0:
				z_arr.append(x[0])
			elif x[1] == 1:
				z_arr.append("sum(" + x[0] + ")")
			elif x[1] == 2:
				z_arr.append("avg(" + x[0] + ")")
			elif x[1] == 3:
				z_arr.append("max(" + x[0] + ")")
			elif x[1] == 4:
				z_arr.append("min(" + x[0] + ")")
			elif x[1] == 5:
				z_arr.append("count(" + x[0] + ")")
		req_arr = z_arr  
	if len(final_arr) == 0:
		print("No Rows Found")
		return
	else:
		for i in range(len(req_arr)):
			if i == len(req_arr) - 1:
				print(str(req_arr[i]) + "\n", end = '')
			else:
				print(str(req_arr[i]) + ", ", end = '')
		for x in final_arr:
			for i in range(len(x)):
				if i == len(x) - 1:
					print(str(x[i]) + "\n", end = '')
				else:
					print(str(x[i]) + ", ", end = '')


def parse_query(query):
	if ";" not in query:
		print("Error : Missing ;")
		return
	if ("select" not in query) and ("SELECT" not in query) and ("Select" not in query):
		print("Error : Missing select statement")
		return
	if ("from" not in query) and ("FROM" not in query) and ("From" not in query):
		print("Error : Missing FROM statement")
		return
	
	query = re.split(";", query)[0]
	fdelim = ["from", "FROM", "From"]
	sdelim = ["select", "SELECT", "Select"]
	wdelim = ["where","WHERE", "Where"]
	adelim = ["and", "And", "AND"]
	odelim = ["or", "Or", "OR"]
	compdelim = [">=", "<=", "<", ">", "="]
	orderdelim = ["order by", "ORDER BY", "Order By"] # Select col1,col2 from table_name order by col1 ASC|DESC.
	order2delim = ["ASC", "asc", "Asc"]
	order3delim = ["DESC", "desc", "Desc"]
	groupdelim = ["Group By", "group by", "GROUP BY"]

	regpatt = '|'.join(map(re.escape, fdelim)) # regpatt: regex pattern
	temp1 = re.split(regpatt, query)
	# print(temp1)

	regpatt = '|'.join(map(re.escape, sdelim))
	attr = temp1[0]
	attr = re.split(regpatt, attr)[1] # " *"
	attr = re.split(" ", attr) # "*"
	# print(attr) # What to select 

	regpatt = '|'.join(map(re.escape, wdelim))
	tab_attr1 = re.split(regpatt, query)[0]
	# print(tab_attr1) # all the stuff before "where"

	##### ############# Getting Table Name ###########
	regpatt = '|'.join(map(re.escape, fdelim))
	tab_attr = re.split(regpatt, tab_attr1)[1] 
	
	regpatt = '|'.join(map(re.escape, orderdelim))
	tab_attr = re.split(regpatt, tab_attr)[0] 

	regpatt = '|'.join(map(re.escape, groupdelim))
	tab_attr = re.split(regpatt, tab_attr)[0]

	tab_attr = re.split(",", tab_attr)
	# print(tab_attr) # Name of the table 
	################################################

	# taking care of ORDER BY
	order_attr = []
	for i in orderdelim:
		if i in tab_attr1:
			regpatt = '|'.join(map(re.escape, orderdelim))
			order_attr = re.split(regpatt, tab_attr1)[1]
			order_attr = re.split(" ", order_attr)
			break

	# taking care of GROUP BY
	group_attr = []
	for i in groupdelim:
		if i in tab_attr1:
			regpatt = '|'.join(map(re.escape, groupdelim))
			group_attr = re.split(regpatt, tab_attr1)[1]
			group_attr = re.split(" ", group_attr)
			break

	# print(group_attr) # ['', 'G']

	t_attr = []
	if len(tab_attr) == 0:
		print("Error : Insufficient number of tables")
		return
	if len(attr) == 0:
		print("Error : Insufficient number of columns")
		return
	t_attr, attr = get_attr(tab_attr, attr) # ['table3'] ['F', 'count(G)', 'max(H)']
	# print("t_attr, attr: ", t_attr, attr)

	if t_attr == False:
		return
	
	curr_columns = []
	curr_arr = []
	new_arr = []
	new_arr = join_tables(t_attr, curr_columns)
	# print("CURR COL", curr_columns)

	agg_flag = 0
	nagg_flag = 0
	distinct_flag = 0
	all_flag = 0
	c_flag = 0
	order_flag = 0
	group_flag = 0
	req_arr = []

	if not order_attr:
		order_flag = 0
	else:
		order_flag = 1

	if not group_attr:
		group_flag = 0
	else:
		group_flag = 1

	########### Order By Checking ##################################
	if order_flag == 1:
		final_order_attr = []

		if len(order_attr) != 3:
			print("Check arguments")
			return

		par, q = check_attr(order_attr[1], t_attr)
		if par == False:
			print("Column not present in table")
			return

		final_order_attr.append(q)

		if order_attr[2] in order2delim:
			final_order_attr.append(order_attr[2])
		elif order_attr[2] in order3delim:
			final_order_attr.append(order_attr[2])
		else:
			print("Enter Asc or Desc")
			return
		# print("Final Order Attr ", final_order_attr) # ['table1.B', 'Desc']

	########### Group By Checking ##################################
	if group_flag == 1:
		final_group_attr = []

		if len(group_attr) != 2:
			print("Check arguments")
			return

		par, q = check_attr(group_attr[1], t_attr)
		if par == False:
			print("Column not present in table")
			return

		final_group_attr.append(q)

		# print("Final Group Attr ", final_group_attr) # ['table1.B', 'Desc']
	
	##############################################################

	for x in attr:    
	  if x == "*":
		  c_flag = 1

	if c_flag == 1:
	  all_flag = 1
	  for x in attr:
	  	if x == "*":
	  		continue
	  	if "(" in x and group_flag == 0:
	  		print("Error : * cannot be combined with aggregate functions")
	  		return
	  	par, q = check_attr(x, t_attr)
	  	if par == False:
	  		return

	else: # no * is there 
	  for x in attr:
		  if x == "distinct":
			  distinct_flag = 1
			  continue
		  if "(" in x:
			  agg_flag = 1
			  y = re.split("\(", x)
			  var = 1
			  # print(y) # ['sum', 'A)']
			  
			  # var stores what aggregate function is being called
			  if y[0] == "sum":
				  var = 1
			  elif y[0] == "avg":
				  var = 2
			  elif y[0] == "max":
				  var = 3
			  elif y[0] == "min":
				  var = 4
			  elif y[0] == "count":
			  	  var = 5
			  else:
			  	print("Please enter correct aggregate function")
			  	return

			  
			  y[1] = re.split("\)", y[1])[0]
			  par, y[1] = check_attr(y[1], t_attr)
			  if par == True:
			  	req_arr.append((y[1], var)) # req_arr = (A, 1)
			  else:
			  	return
		  else:
			  nagg_flag = 1
			  par, x = check_attr(x, t_attr)
			  if par == True:
			  	req_arr.append((x, 0)) # 0 indicates no aggregation operation 
			  else:
			  	return
	
	if agg_flag == 1 and nagg_flag == 1 and group_flag == 0:
		print("Error : Aggregate functions cannot be combined with columns, unless you're using group by ")
		return
	
	try:
		# fj_arr = []
		cond_arr = [] # condition array
		
		o_flag = 0
		a_flag = 0
		
		regpatt = '|'.join(map(re.escape, wdelim))
		join_cond = re.split(regpatt, query)[1]
		# print("join_cond: ", join_cond) # the condition after WHERE
		
		### Multiple conditions
		if ("and" in join_cond) or ("And" in join_cond) or ("AND" in join_cond):
			a_flag = 1
			regpatt = '|'.join(map(re.escape, adelim))
			cond_arr = re.split(regpatt, join_cond)
		elif "or" in join_cond:
			o_flag = 1
			regpatt = '|'.join(map(re.escape, odelim))
			cond_arr = re.split(regpatt, join_cond)
		else:
			cond_arr.append(join_cond) 
		# print("cond_arr: ", cond_arr) #[' A=922 ', ' B=158']

		# fj_arr = create_final(cond_arr, t_attr)
		fj_arr = []
		for join_str in cond_arr:
			regpatt = '|'.join(map(re.escape, compdelim))
			tj = re.split(regpatt, join_str)
			join_arr = []
			
			for z in tj:
				c = re.split(" ", z)
				for w in c:
					if len(w) > 0:
						join_arr.append(w)

			j_arr = []
			for x in join_arr:
				if is_number(x):
					j_arr.append(x)
					continue
				par, x = check_attr(x, t_attr)
				if par == True:
					j_arr.append(x)
				else:
					return

			t_var = 0
			if "<=" in join_str:
				t_var = 1
			elif "<" in join_str:
				t_var = 2
			elif ">" in join_str:
				t_var = 3
			elif "=" in join_str:
				t_var = 4
			else:
				continue

			fj_arr.append([j_arr[0], j_arr[1], t_var])

		# print("Final Join: ", fj_arr) # Final Join: [ ['table1.A', '922', 4], ['table1.B', '158', 4] ]]

		if a_flag == 1 or o_flag == 1:
			if len(fj_arr) != 2:
				print("Error : Wrong use of and/or keyword")
				return
			
			fn_arr = []
			for x in new_arr:
				flag = [1, 1]
				for i in range(2):
					# Where (1==1)
					if is_number(fj_arr[i][0]) == True and is_number(fj_arr[i][1]) == True:
						a = float(fj_arr[i][0])
						b = float(fj_arr[i][1])
						flag[i] = check(a, b, fj_arr[i][2])
					# where 1 = A 
					elif is_number(fj_arr[i][0]) == True and is_number(fj_arr[i][1]) == False:
						a = float(fj_arr[i][0])
						ind = 0
						for z in range(len(curr_columns)):
							if curr_columns[z] == fj_arr[i][1]:
								ind = z
								break
						b = float(x[ind])
						flag[i] = check(a, b, fj_arr[i][2])
					
					# where A =1 
					elif is_number(fj_arr[i][0]) == False and is_number(fj_arr[i][1]) == True:
						b = float(fj_arr[i][1])
						ind = 0
						for z in range(len(curr_columns)):
							if curr_columns[z] == fj_arr[i][0]:
								ind = z
								break
						a = float(x[ind])
						flag[i] = check(a, b, fj_arr[i][2])
					
					# where A = B 
					else:
						ind = 0
						for z in range(len(curr_columns)):
							if curr_columns[z] == fj_arr[i][0]:
								ind = z
								break
						f1 = ind
						a = float(x[ind])
						ind = 0
						for z in range(len(curr_columns)):
							if curr_columns[z] == fj_arr[i][1]:
								ind = z
								break
						f2 = ind
						b = float(x[ind])
						flag[i] = check(a, b, fj_arr[i][2])
				
				if a_flag == 1 and flag[0] and flag[1]:
					fn_arr.append(x)
				elif o_flag == 1 and (flag[0] or flag[1]):
					fn_arr.append(x) # FINAL ARRAY, which we are going to print 
			new_arr = fn_arr
		
		else:
			if len(fj_arr) != 1:
				print("Error : Invalid use of where clause")
				return
			
			fn_arr = []
			for x in new_arr:
				flag = 1
				if is_number(fj_arr[0][0]) == True and is_number(fj_arr[0][1]) == True:
					a = float(fj_arr[0][0])
					b = float(fj_arr[0][1])
					flag = check(a, b, fj_arr[0][2])
				
				elif is_number(fj_arr[0][0]) == True and is_number(fj_arr[0][1]) == False:
					a = float(fj_arr[0][0])
					ind = 0
					for z in range(len(curr_columns)):
						if curr_columns[z] == fj_arr[0][1]:
							ind = z
							break
					b = float(x[ind])
					flag = check(a, b, fj_arr[0][2])
				
				elif is_number(fj_arr[0][0]) == False and is_number(fj_arr[0][1]) == True:
					b = float(fj_arr[0][1])
					ind = 0
					for z in range(len(curr_columns)):
						if curr_columns[z] == fj_arr[0][0]:
							ind = z
							break
					a = float(x[ind])
					flag = check(a, b, fj_arr[0][2])
				
				else:
					ind = 0
					for z in range(len(curr_columns)):
						if curr_columns[z] == fj_arr[0][0]:
							ind = z
							break
					f1 = ind
					a = float(x[ind])
					ind = 0
					for z in range(len(curr_columns)):
						if curr_columns[z] == fj_arr[0][1]:
							ind = z
							break
					f2 = ind
					b = float(x[ind])
					flag = check(a, b, fj_arr[0][2])
				
				if flag:
					fn_arr.append(x)
			new_arr = fn_arr
	except:
		pass
	
	if all_flag == 1: # when we are selecting *
		try:
			if a_flag == 1 or o_flag == 1:
				curr_columns, new_arr = handle_join(fj_arr, curr_columns, new_arr, 2)
			else:
				curr_columns, new_arr = handle_join(fj_arr, curr_columns, new_arr, 1)
		except:
			pass

		if distinct_flag == 0:

			#### ORDER BY #################################################
			if order_flag == 1: # That means before printing it, we need to sort!
				
				# Get index of columns
				for i in range(len(curr_columns)):
					if curr_columns[i] == final_order_attr[0]:
						ind = i #which index is the col present
						break
				
				## Now we need to sort final array
				if final_order_attr[1] in order2delim:
					new_arr.sort(key=lambda x: x[ind])
				else:
					new_arr.sort(key=lambda x: x[ind], reverse=True)
			################################################################
			print_result(curr_columns, new_arr)
		else:
			y = []
			for x in new_arr:
				if x not in y:
					y.append(x)
			print_result(curr_columns, y)
	
	else: ### We are printing selected columns (no *) 

		if nagg_flag == 1: # There is no aggregation involved
			final_arr = []
			# print("New Array: ", new_arr) # the rows of the table
			# print("Reqd Array: ", req_arr) 
			# [('table1.A', 0), ('table1.B', 0)] what operation is being done on what col
			# print("Curr Columns Array: ", curr_columns) # ['table1.A', 'table1.B', 'table1.C']
			for x in new_arr:
				temp_arr = []
				for init in range(len(req_arr)):
					for i in range(len(curr_columns)):
						if curr_columns[i] == req_arr[init][0]:
							temp_arr.append(x[i])
							break
				final_arr.append(temp_arr)

			# print("final Array: ", final_arr) # the selected rows from the selected columns
			try:
				if a_flag == 1 or o_flag == 1:
					curr_columns, final_arr = handle_join(fj_arr, curr_columns, final_arr, 2)
				else:
					curr_columns, final_arr = handle_join(fj_arr, curr_columns, final_arr, 1)
			except:
				pass

			if distinct_flag == 0: # No distinct keyword
				
				#### ORDER BY #################################################
				if order_flag == 1: # That means before printing it, we need to sort!
					
					# Get index of columns
					for i in range(len(curr_columns)):
						if curr_columns[i] == final_order_attr[0]:
							ind = i #which index is the col present
							break
					
					## Now we need to sort final array
					if final_order_attr[1] in order2delim:
						final_arr.sort(key=lambda x: x[ind])
					else:
						final_arr.sort(key=lambda x: x[ind], reverse=True)
				################################################################

				##### GROUP BY ###############################################
				# group_dict = {}
				elif group_flag == 1: # here both agg_flag and nagg_flag are 1 
					# print("FINAL ARRAY GROUP START: ", final_arr)
					final_arr = []
					for i in range(len(curr_columns)):
						if curr_columns[i] == final_group_attr[0]:
							ind = i # which index to group by 
							break
					# final_arr.sort(key=lambda x: x[ind])

					final_tarr = []
					ctr1 = 0
					ctr2 = 0
					for x in req_arr: # [('table3.F', 0), ('table3.G', 5), ('table3.H', 4))]
						group_dict = {}
						if x[1] == 0:
							continue ## non aggrgation function, don't need those 
						
						for i in new_arr:
							cur_key = i[ind]
							if cur_key not in group_dict:
								group_dict[cur_key] = []

							ind2 = 0
							for j in range(len(curr_columns)):
								if x[0] == curr_columns[j]:
									ind2 = j
									break

							group_dict[cur_key].append(i[ind2])

						# print("GROUP DICT:", group_dict) #This worked :)
						## Now apply aggregation
						# final_tarr = []
						
						if x[1] == 1 or x[1] == 2 or x[1] == 5:
							for y in group_dict: # looping over the dict keys
								# print(group_dict[y])
								tarr = []
								val = 0
								cnt = 0
								for z in group_dict[y]:
									# print(int(z))
									val += int(z)
									cnt += 1

								tarr.append(int(y))
								if x[1] == 1 and len(group_dict[y]) > 0:
									tarr.append(val)    
								elif x[1] == 2 and len(group_dict[y]) > 0: 
									tarr.append(val / cnt)
								elif x[1] == 5: 
									tarr.append(cnt)

								if ctr1 == 0:
									final_tarr.append(tarr)
								else:
									for m in final_tarr:
										if m[0] == int(y):
											m.append(tarr[1])
						
						if x[1] == 3 or x[1] == 4:
							for y in group_dict: # looping over the dict keys
								# print(group_dict[y])
								tarr = []
								val = -10000000
								val2 = 1000000000

								for z in group_dict[y]:
									val = max(val, int(z))
									val2 = min(val, int(z))
									# print(int(z))

								tarr.append(int(y))

								if x[1] == 3 and len(group_dict[y]) > 0: 
									tarr.append(val)   

								if x[1] == 4 and len(group_dict[y]) > 0: 
									tarr.append(val2) 

								if ctr2 == 0:
									final_tarr.append(tarr)
								else:
									for m in final_tarr:
										if m[0] == int(y):
											m.append(tarr[1])

						ctr1 += 1
						ctr2 += 1

					# print("FINAL TARR: ", final_tarr)
					final_arr = final_tarr

				###############################################################
				
				print_result(req_arr, final_arr)
			
			else: # For Distinct
				f_arr = []
				for x in final_arr:
					if x not in f_arr:
						f_arr.append(x)
				print_result(req_arr, f_arr)
		
		else: # For just aggregate functions without group by
			final_arr = []
			tarr = []
			for x in req_arr: 
				ind = 0
				for i in range(len(curr_columns)):
					if x[0] == curr_columns[i]:
						ind = i #which index is the col present
						break
				if x[1] == 1 or x[1] == 2 or x[1] == 5:
					val = 0
					cnt = 0
					for y in new_arr:
						val += int(y[ind])
						cnt += 1
					if x[1] == 1 and len(new_arr) > 0:
						tarr.append(val)    
					elif x[1] == 2 and len(new_arr) > 0: 
						tarr.append(val / cnt)
					elif x[1] == 5: # make this zro
						tarr.append(cnt)
				elif x[1] == 3:
					val = -10000000
					for y in new_arr:
						val = max(val, int(y[i]))
					if len(new_arr):
						tarr.append(val)
				elif x[1] == 4:
					val = 1000000000
					for y in new_arr:
						val = min(val, int(y[i]))
					if len(new_arr):
						tarr.append(val)

			final_arr.append(tarr)
			try:
				if a_flag == 1 or o_flag == 1:
					curr_columns, final_arr = handle_join(fj_arr, curr_columns, final_arr, 2)
				else:
					curr_columns, final_arr = handle_join(fj_arr, curr_columns, final_arr, 1)
			except:
				pass
			print_result(req_arr, final_arr)

getdata()
parse_query(sys.argv[1])
