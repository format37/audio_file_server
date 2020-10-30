import pymysql as sql

class cook_object:

	def __init__(self):

		self.db_name = 'MICO_96'
		self.db_server = '10.2.4.146'
		self.db_login = 'asterisk'
		with open('sql.pass','r') as file:
			self.db_pass = file.read().replace('\n', '')
			file.close()
		self.con = self.connect_sql()

	def connect_sql(self):

		return sql.connect(
			self.db_server, 
			self.db_login, 
			self.db_pass,
			self.db_name
		)

def cdr_filenames(df):

	cook = cook_object()

	with cook.con:
		# drop temporary tables
		query = "drop table if exists primary_linkedid_0;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists primary_linkedid_1;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists primary_linkedid_2;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists linkedid_relation_batch_1_instance_0;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists linkedid_relation_batch_1_instance_1;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists linkedid_relation_batch_1_instance_2;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists linkedid_relation_batch_2_instance_0;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = "drop table if exists linkedid_related;"
		cursor = cook.con.cursor()
		cursor.execute(query)

		# create known linkedid list
		query = 'create temporary table primary_linkedid_0'
		#i = 0
		for i in range(len(df)):
			instance = df.iloc[i]
			#print(row.call_id,row.linkedid)
			print('instance.date_from',instance.date_from)
			query+="""
			"""+('' if i==0 else 'union all')+"""
			select
			'"""+str(instance.call_id)+"""' as call_id,
			'"""+str(instance.date_from)+"""' as date_from,
			'"""+str(instance.date_to)+"""' as date_to,
			'"""+str(instance.linkedid)+"""' as linkedid"""
		'''
		for instance in linkedid:
			query+="""
			"""+('' if i==0 else 'union all')+"""
			select
			'"""+instance['call_id']+"""' as call_id,
			'"""+instance['date_from']+"""' as date_from,
			'"""+instance['date_to']+"""' as date_to,
			'"""+instance['linkedid']+"""' as linkedid"""
			i+=1
		'''
		query+=';'
		query
		cursor = cook.con.cursor()
		cursor.execute(query)

		# duplicate
		query = """create temporary table primary_linkedid_1
		select
		call_id,
		date_from,
		date_to,
		linkedid
		from primary_linkedid_0;"""    
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = """create temporary table primary_linkedid_2
		select
		call_id,
		date_from,
		date_to,
		linkedid
		from primary_linkedid_0;"""
		cursor = cook.con.cursor()
		cursor.execute(query)

		# union -> relation batch 1
		query = """create temporary table linkedid_relation_batch_1_instance_0
		select 
		primary_linkedid_0.call_id,
		primary_linkedid_0.date_from,
		primary_linkedid_0.date_to,
		primary_linkedid_0.linkedid,
		0 as relation_level
		from primary_linkedid_0 as primary_linkedid_0
		union all

		select
		primary_linkedid_1.call_id,
		primary_linkedid_1.date_from,
		primary_linkedid_1.date_to,
		relationsCalls.linkedid1,
		1
		from primary_linkedid_1 as primary_linkedid_1
		inner join relationsCalls 
		on 
		relationsCalls.calldate>primary_linkedid_1.date_from and
		relationsCalls.calldate<primary_linkedid_1.date_to and
		primary_linkedid_1.linkedid=relationsCalls.linkedid2
		union all

		select
		primary_linkedid_2.call_id,
		primary_linkedid_2.date_from,
		primary_linkedid_2.date_to,
		relationsCalls.linkedid2,
		1    
		from primary_linkedid_2 as primary_linkedid_2
		inner join relationsCalls 
		on 
		relationsCalls.calldate>primary_linkedid_2.date_from and
		relationsCalls.calldate<primary_linkedid_2.date_to and
		primary_linkedid_2.linkedid=relationsCalls.linkedid1;
		"""
		cursor = cook.con.cursor()
		cursor.execute(query)


		# duplicate relation batch 1
		query = """create temporary table linkedid_relation_batch_1_instance_1
		select 
		call_id,
		date_from,
		date_to,
		linkedid,
		relation_level
		from linkedid_relation_batch_1_instance_0;"""
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = """create temporary table linkedid_relation_batch_1_instance_2
		select 
		call_id,
		date_from,
		date_to,
		linkedid,
		relation_level
		from linkedid_relation_batch_1_instance_0;"""
		cursor = cook.con.cursor()
		cursor.execute(query)

		# union -> relation batch 2
		query = """create temporary table linkedid_relation_batch_2_instance_0    
		select 
		linkedid_relation_batch_1_instance_0.call_id,
		linkedid_relation_batch_1_instance_0.date_from,
		linkedid_relation_batch_1_instance_0.date_to,
		linkedid_relation_batch_1_instance_0.linkedid
		from linkedid_relation_batch_1_instance_0 as linkedid_relation_batch_1_instance_0    
		union all

		select 
		linkedid_relation_batch_1_instance_1.call_id,
		linkedid_relation_batch_1_instance_1.date_from,
		linkedid_relation_batch_1_instance_1.date_to,
		relationsCalls.linkedid2
		from linkedid_relation_batch_1_instance_1 as linkedid_relation_batch_1_instance_1
		inner join relationsCalls 
		on 
		relationsCalls.calldate>linkedid_relation_batch_1_instance_1.date_from and
		relationsCalls.calldate<linkedid_relation_batch_1_instance_1.date_to and
		linkedid_relation_batch_1_instance_1.linkedid=relationsCalls.linkedid1
		where 
		linkedid_relation_batch_1_instance_1.relation_level = 1
		union all

		select 
		linkedid_relation_batch_1_instance_2.call_id,
		linkedid_relation_batch_1_instance_2.date_from,
		linkedid_relation_batch_1_instance_2.date_to,
		relationsCalls.linkedid1
		from linkedid_relation_batch_1_instance_2 as linkedid_relation_batch_1_instance_2
		inner join relationsCalls 
		on 
		relationsCalls.calldate>linkedid_relation_batch_1_instance_2.date_from and
		relationsCalls.calldate<linkedid_relation_batch_1_instance_2.date_to and
		linkedid_relation_batch_1_instance_2.linkedid=relationsCalls.linkedid2
		where 
		linkedid_relation_batch_1_instance_2.relation_level = 1;
		"""
		cursor = cook.con.cursor()
		cursor.execute(query)

		# group related linkedid
		query = """create temporary table linkedid_related
		select distinct    
		call_id,
		date_from,
		date_to,
		linkedid COLLATE utf8_unicode_ci as linkedid
		from linkedid_relation_batch_2_instance_0"""
		cursor = cook.con.cursor()
		cursor.execute(query)

		query = """
		select
		linkedid_related.call_id,    
		linkedid_related.linkedid,
		PT1C_cdr_MICO.recordingfile
		from PT1C_cdr_MICO as PT1C_cdr_MICO
		inner join linkedid_related as linkedid_related on
		PT1C_cdr_MICO.calldate>linkedid_related.date_from and
		PT1C_cdr_MICO.calldate<linkedid_related.date_to and
		PT1C_cdr_MICO.linkedid = linkedid_related.linkedid"""

		cursor = cook.con.cursor()
		cursor.execute(query)

		return cursor.fetchall()
