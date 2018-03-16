__author__ = 'Hunter'

def check_table_exists(dbconn, tablename):
	dbcur = dbconn.cursor()
	dbcur.execute("SELECT COUNT(*) FROM Kaggle.tables WHERE table_name = %s", ('%s' % tablename,))
	dbcur.execute("""SELECT table_name FROM kaggle.tables WHERE table_schema = 'public'""")
	if dbcur.fetchone()[0] == 1:
		dbcur.close()
		return True

	dbcur.close()
	return False