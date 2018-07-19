<?php
/*
Date        Ver  Who  Change
----------  ---  ---  -----------------------------------------------------
            1.0  FHO  Class isolated from existing project
2015-08-14  1.1  FHO  camelCase
2016-05-30  1.2  FHO  added some prototyping
                      added recording functions
                      added transactionCount to avoid subtransactions (mySql does not support subtransactions
                      BEGIN TRANS inside a transaction will commit current one and start a new one)
*/

// Database connexion pool
// example:
// $dbo = dbCnxPool::getPDO([ 'engine' => 'mysql', 'host' => 'localhost', 'username' => 'dbuser', 'passwd' => 'dbpassword', 'database' => 'mdb']);
// $dbo = dbCnxPool::getPDO([ 'dsn' => 'mysql:dbname=mdb;host=localhost', 'username' => 'dbuser', 'passwd' => 'dbpassword' ]);
// at first call, will create a PDO object
// subsequent calls for same 'database' value will return previously created object

class dbCnxPool
{
	private static $dbLinks;
	private static $lastError;	// message
	private static $errorCode;	// code

	/**
	* Constructeur de la classe
	*
	* @param void
	* @return void
	*/
	private function
	__construct() {  
	}

	/**
	* getPDO
	* Retrieve a PDO object for a given database
	*
	* @param array Connexion info (database, engine, host, dsn, username, passwd)
	* @return object PDO for this database or false if failed
	*/
	public static function
	getPDO ($args) {
		// 1st call: create connexion pool
		if (is_null(self::$dbLinks)) {
			self::$dbLinks = array();
		}

		// search for this database
		if (isset (self::$dbLinks[$args['database']]))
			return self::$dbLinks[$args['database']]['pdo'];

		try
		{
			if (isset ($args['dsn']) )
				$dsn = $args['dsn'];
			else
				$dsn = $args['engine'].':dbname='.$args['database'].";host=".$args['host'];
			$pdo = new PDO( $dsn, $args['username'], $args['passwd']);
		}
		catch (PDOException $e)
		{
			self::$errorCode = $e -> getCode();
			self::$lastError = $e -> getMessage();
			return false;
		}
		$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION); // Set Errorhandling to Exception

		self::$dbLinks[$args['database']] = $args;
		self::$dbLinks[$args['database']]['dsn'] = $dsn;
		self::$dbLinks[$args['database']]['pdo'] = $pdo;

		return $pdo;
	}

	/**
	 * Get the message associated with last error encountered
	 * @return string Text of last error
	 */
	public static function
	getLastError (): string
	{
		return self::$lastError;
	}

	/**
	 * Close a connection in the pool identified by its database name
	 * Closing means deleting the pdo object here, there is no guarantee that the link
	 * is physically closed, depends on implementation and configuration
	 * @param string database Name of the database. If non existent, will do nothing.
	 * @return void Nothing
	 */
	public static function
	close (string $database): void
	{
		if (isset (self::$dbLinks[$database]))
		{
			// close database
			self::$dbLinks[$database]['pdo'] = null;

			// clear infos
			unset (self::$dbLinks[$database]);
		}
	}

	/**
	 * Close all connections in the pool
	 */
	public static function
	closeAll (): void
	{
		foreach (self::dbLinks as $database => $dbLink)
			self::close($database);
	}
}

class fwpdo
{
	use errorhandler;

	private $onerror;

	private $database;
	private $transactionCount;

	private $timestart;
	private $time_end;

	public $sql;
	public $success;
	public $nrows;
	public $duration;

	private $errorCode;	// error code
	public $shortmsg;	// error message without SQL statement

	// Read only mode
	private $readOnly;

	// Recording
	private $recording;
	private $sqls;

	//__construct($host, $username, $passwd, $database)
	public function
	__construct()
	{
		switch (func_num_args())
		{
		case 4 : // f($host, $username, $passwd, $database)
			$args = self::buildArgsForOldSyntax(func_get_args(),['host','username', 'passwd', 'database' ] );
			break;

		case 1 : // f([])
			$args = func_get_arg(0);
			if (!is_array ($args))
			{
				$this -> setTemporaryHandler('exception');
				return $this -> errorHandler('Syntax error');
			}
			break;
		}

		$this -> recording = false;
		$this -> readOnly = false;

		// set default values for unset parms
		$args = self::addMissing($args, [ 'engine' ], 'mysql' );
		$args = self::addMissing($args, [ 'onerror' ], 'die' );

		// switch to user requested error handler
		$this -> setTemporaryHandler($args['onerror']);

		$this -> database = $args['database'];
		$this -> pdo = dbCnxPool::getPDO($args);

		// this is likely no transaction has been started
		// but, due to connexion pool, it is not sure
		// if a transaction is started but neither committed or rollback
		// it will remain open
		// (it should be rollbacked)
		$this -> transactionCount = 0;

		if ($this -> pdo == '')
			return $this -> errorHandler(dbCnxPool::getLastError());

		$this -> resetTemporaryHandler();
	}

	//==================================================
	//
	//             ERROR MANAGEMENT
	//
	//==================================================

	public function
	getLastError($long = true): string
	{
		if ($long)
			return $this -> msg;
		else
			return $this->shortmsg;
	}

	public function
	getErrorCode ()
	{
		return $this -> errorCode;
	}

	//==================================================
	//
	//             FIELD MANAGEMENT
	//
	//==================================================

	/**
	 * Returns a formatted date time field for now
	 * @return string SQL formatted date time
	 */
	public function
	dateTime(): string
	{
		date_default_timezone_set('Europe/Paris');
		return date("Y-m-d H:i:s");
	}

	// format a field value (add quotes, escape etc.)
	// $fielfValue can be null so cannot be types as string
	private function
	formatFieldValue ($fieldValue, bool $trimall = false): string
	{
		if (is_null($fieldValue))
			$str = 'NULL';
		else
		{
			if ($trimall)
				$fieldvalue = trim($fieldvalue);
			$str = $this->quote($fieldValue) ;
		}

		return $str;
	}

	// format a field name
	private function
	formatFieldName (string $fieldName): string
	{
		return '`' . $fieldName . '`';
	}

	//==================================================
	//
	//             INFO
	//
	//==================================================

	/**
	 * Returns the number of current database
	 * @return string Name of database
	 */
	public function
	getDatabase(): string
	{
		return $this -> database;
	}

	/**
	 * Returns the number of rows read/touched during last call
	 * @return int Number of rows
	 */
	public function
	numRows(): int
	{
		return $this -> nrows;
	}

	/**
	 * Builds the WHERE statement of an SQL query
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @return string WHERE part of an SQL statement
	 */
	// 'x > 3'                 ==> WHERE x > 3
	// 'x IS NULL'             ==> WHERE x IS NULL
	// array('x > 3', 'y > 5') ==> WHERE x > 3 AND y > 5
	// array('x' => 3)         ==> WHERE x = 3
	// array('x' => 3, 'y > 5', array('col'=>'z', 'op'=>'<>', 'val'=>'10')
	//                         ==> WHERE x = 3 AND y > 5 AND z <> 10

	private function
	buildWhereStatement ($whereparm, $boolop = 'AND'): string
	{
		return $this -> buildWhereOrHavingStatement($whereparm, 'WHERE', $boolop);
	}
	private function
	buildHavingStatement ($havingparm, $boolop = 'AND'): string
	{
		return $this -> buildWhereOrHavingStatement($havingparm, 'HAVING', $boolop);
	}
	/**
	 * Builds a WHERE or HAVING statement
	 * @param mixed parm WHERE or HAVING parm
	 * @param string statement WHERE or HAVING
	 * @boolop string AND|OR
	 * @returns string sql statement
	*/
	private function
	buildWhereOrHavingStatement ($parm, string $statement, $boolop): string
	{
		if ($parm == '')
			return '';

		if (is_array($parm))
			$arr = $parm;
		else
		{
			if ($this -> startsWith ($parm, $statement, false))
				$parm = substr ($parm, strlen($statement));

			$arr = array($parm);
		}

		$sql = '';
		foreach ($arr as $col=>$item)
		{
			// when array item is named, then $col is a string - is_int() is false
			// when it is not names, $col is a numeric value - is_int() is true
			// is_int (12)  => true
			// is_int('12') => false

			if (is_int ($col))	// no name
			{
				if (is_array ($item))
					$part = $this->formatFieldName($item['col']) . $item['op'] . $this->formatFieldValue($item['value']) ;
				else
					$part = $item;
			}
			else	// named
				// caution: $col must *not* use formatFieldName() here
				if (is_null ($item))
					$part = $col . ' IS NULL';
				else
					if (is_array($item))	// Added 2017-11-25
					{
						$part = '';
						$part .= $col;
						$part .= ' IN ';
						$part .= '(';
						$valuesarray = [ ];
						foreach ($item as $invalue)
							$valuesarray[] = $this->formatFieldValue($invalue);
						$part .= implode (',' , $valuesarray);
						$part .= ')';
					}
					else
						$part = $col . '=' . $this->formatFieldValue($item);

			if ($sql == '')
				$sql .= " \n" . $statement . ' ';
			else
				$sql .= "\n  ".$boolop." ";

			$sql .= $part;
		}

		return $sql;
	}

	private function
	startSql ($sql)
	{
		// Reset infos from prev request
		$this -> msg = '';
		$this -> shortmsg = '';
		$this -> nrows = 0;
		$this -> duration = 0;

		$this -> sql = $sql;
		$this -> time_start = microtime(true);
		$this -> success = false;
	}

	private function
	stopSql ()
	{
		$this -> time_end   = microtime(true);
		$this -> duration   = $this -> time_end - $this -> time_start;
		$this -> record();
	}

	private function
	record ()
	{
return;
		date_default_timezone_set('Europe/Paris');
		$str = date('Y-m-d H:i:s');
		$str .= ' SQL=[' . $this -> sql . ']';
		$str .= ' duration=[' . sprintf('%.2f', $this -> duration) . ' ms]';
		$str .= ' rows=' . $this->nrows;
		$str = str_replace ("\n", ' ', $str);

		$fp = @fopen ('/tmp/sql.log', 'a');
		if ($fp)
		{
			fwrite ($fp, $str . "\n");
			fwrite ($fp, "---------------------------------------------------------------------------\n");
			fclose ($fp);
		}
	}

	/**
	 * Sets DB engine charset
	 * @param string name of charset
	 * @returns bool true if success, false if failure
	 */
	public function
	setCharset (string $charset)
	{
		return $this -> executeSql ('SET NAMES ' . $charset);
	}

	// if success, returns true
	// if failure, behaviour depends on $onerror parm
	//             if $onerror parm unset, then based on $this->onerror
	//             'die' -> write a message on stdout and die
	//             'return' -> return false
	//             'raise','exception' -> raise an exception
	//             'user' -> call function defined by setHandler() and return its return value
	// forceExec: if true executes statement, overrinding read-only mode
	private function
	executeSql (string $sql, string $onerror = '', bool $forceExec = false)
	{
		if ($this -> recording)
			$this -> doRecord ($sql);

		if ($forceExec || !$this -> readOnly)
		{
			$this -> startSql ($sql);
			$this -> success = false;
			try
			{
				$this -> nrows = $this->pdo->exec($sql);
				$this -> success = true;
			}
			catch (PDOException $e)
			{
				$this->errorCode = $e -> getCode();
				$this->shortmsg = $e -> getMessage();
				$this->msg = $sql . ': ' . $e -> getMessage();
			}
			// finally
			$this -> stopSql ();

			if ($this -> success)
				return true;

			$this -> setTemporaryHandler ($onerror);
			return $this -> errorHandler ($this->msg);
		}
		else
			$this -> doRecord ($sql);
	}

	private function
	must_executeSql (string $sql)
	{
		$this -> executeSql($sql, 'die');
	}

	private function
	fetch (string $sql, string $onerror='')
	{
		$this -> startSql ($sql);
		try
		{
			$this -> success = false;
			$st = $this->pdo->query($sql);
			$this->nrows = $st -> rowCount();
			$arr = array();
			while (($row = $st -> fetch(PDO::FETCH_ASSOC)))
				$arr[] = $row;
			$this -> success = true;
		}
		catch (PDOException $e)
		{
			$this->errorCode = $e -> getCode();
			$this->shortmsg = $e -> getMessage();
			$this->msg = $sql . ': ' . $e -> getMessage();
			$arr = '';
		}
		// finally
		$this -> stopSql ();

		if ($this -> success)
			return $arr;

		$this -> setTemporaryHandler ($onerror);
		return $this -> errorHandler ($this->msg);
	}

	//--------------------------------------------------------------------
	//                            COUNT
	//--------------------------------------------------------------------

	public function
	count ($fromparm, $whereparm = '', $joinparm = '', $onerror = ''): int
	{
// tracelog ('[fwpdo::count] onError: [' . $onerror . ']');
		$this -> setTemporaryHandler ($onerror);

		if (is_array ($fromparm))	// new syntax
			$args = $fromparm;
		else
		{
			$args = array();
			$args['where'] = $whereparm;
			$args['from']  = $fromparm;
			$args['join']  = $joinparm;
			$args['onerror'] = $onerror;
			$args['boolop']  = 'AND';
		}
		$args['select'] = 'COUNT(*) NB';
		$args['limit']   = '';
		$args['orderby'] = '';
		$args['order']   = '';

		$rows = $this -> select ($args);
//tracelog ('[fwpdo::count] ' . $this->sql);
		if (!$rows || $rows == '' || count($rows) < 1)
			return $this -> errorHandler ('No result');
		else
		{
			$this -> resetTemporaryHandler();
			return $rows[0]['NB'];
		}
	}

	public function
	must_count ($fromparm, $whereparm = '', $joinparm = ''): int
	{
		return $this -> count ($fromparm, $whereparm, $joinparm, 'die');
	}

	//--------------------------------------------------------------------
	//                            SELECT
	//--------------------------------------------------------------------

	// builds args array (new style) when called with 9 parms (old style)
	private function
	buildArgsForOldSelectSyntax ($oldArgs): array
	{
		$parms = [  'select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'group', 'onerror' ];
		return $this -> buildArgsForOldSyntax ($oldArgs, $parms);
	}

	private function
	buildArgsForOldSyntax ($oldArgs, $parms): array
	{
		$num_args = count ($oldArgs);
		if ($num_args > count ($oldArgs))
			return $this -> errorHandler ('too many parameters', 1);

		$newArgs = array();
		$argi = 0;
		foreach ($parms as $parm)
			if ($argi < $num_args)
				$newArgs[$parm] = $oldArgs[$argi++];
			else
				$newArgs[$parm] = '';

		return $newArgs;
	}

	public function
	select ()
	{
		$num_args = func_num_args();

		if ($num_args  == 0)
			return $this -> errorHandler ('fwpdo::select() expects 1 parameter', 1);

		if ($num_args  == 1)	// new syntax
		{
			$args = func_get_arg(0);
			if (!is_array( $args ))
				return $this -> errorHandler ('fwpdo::select() expects an array', 1);
		}
		else // old syntax
		{
//tracelog ('[select] 1 ' . tracelog_dump(func_get_args()) );
			$args = $this -> buildArgsForOldSelectSyntax(func_get_args() );
			if ($args == '')
				return $this -> errorHandler ('fwpdo::select() bad parameters', 1);
		}

//tracelog ('[select] 2 ' . tracelog_dump($args) );
		$sql = $this -> buildSelectRequest($args);
//tracelog ('[select] 1 ' . $sql );
		if ($sql == '')
			return '';

		$rows = $this->fetch ($sql);
		return $rows;
	}

	public function
	select_sql (string $sql): array
	{
		return $this -> fetch ($sql);
	}

	// for compatibility only
	public function
	must_select ($selectparm, $fromparm, $whereparm='', $orderItems="", $limitstr="", $joinparm = '', $sortorder = '', $groupparm = '')
	{
		return $this->select($selectparm, $fromparm, $whereparm, $orderItems, $limitstr, $joinparm, $sortorder, $groupparm, 'die');
	}

	// for compatibility only
	public function
	must_select_sql ($sql)
	{
		return $this -> fetch ($sql, 'die');
	}

	/**
	 * Builds a SELECT statement based on parameters
	 * @param array $args Parms
	 * @return string SQL statement for reading (SELECT)
	 */
	private function
	buildSelectRequest ($args): string
	{
		// Create missing entries, if any

		if (!array_key_exists ('select', $args))
			$args['select'] = '*';

		$args = $this -> addMissing ($args, [ 'select', 'from', 'join', 'where', 'groupby', 'orderby', 'order', 'limit', 'having' ] );
		$args = $this -> addMissing ($args, [ 'boolop' ], 'AND' );
//tracelog ('[buildSelectRequest] ' . tracelog_dump($args));

		//---------------------------------
		// build SQL parts
		//---------------------------------

		$selectStatement  = $this -> buildSelectStatement ($args['select']);
		$fromStatement    = $this -> buildFromStatement ($args['from']);
		$joinStatements   = $this -> buildJoinStatements ($args['join']);
		$whereStatement   = $this -> buildWhereStatement ($args['where'], $args['boolop']);
		$havingStatement  = $this -> buildHavingStatement ($args['having']);
		$groupByStatement = $this -> buildGroupByStatement ($args['groupby']);
		$orderByStatement = $this -> buildOrderByStatement($args['orderby']);
		$orderStatement   = $this -> buildOrderStatement($args['order']);
		$limitStatement   = $this -> buildLimitStatement ($args['limit']);

		//---------------------------------
		// check mandatory ones
		//---------------------------------

		if ($fromStatement == '')
			return $this -> errorHandler ('fwpdo::select() Empty FROM', 1);
		if ($selectStatement == '')
			return $this -> errorHandler ('fwpdo::select() Empty SELECT', 1);

		// Canot use ASC or DESC when no ORDER set
		if ($orderStatement != '' && $orderByStatement == '')
			$orderStatement = '';

		//---------------------------------
		// Altogether
		//---------------------------------

		$sql = $this -> concatStrings(  
			$selectStatement,
			$fromStatement,
			$joinStatements,
			$whereStatement,
			$havingStatement,
			$groupByStatement,
			$orderByStatement,
			$orderStatement,
			$limitStatement  );

		//---------------------------------
		// Return result
		//---------------------------------

		return $sql;
	}

	private function
	buildOrderStatement (string $orderParm): string
	{
		$orderStatement = strtoupper($orderParm);

		return $orderStatement;
	}

	private function
	buildSelectStatement ($selectParm): string
	{
		if (is_array ($selectParm))
			$selectParts = $selectParm;
		else
			$selectParts = array($selectParm);

		$selectStatement = '';
		foreach ($selectParts as $column)
		{
			if ($selectStatement == '')
				$selectStatement .= 'SELECT ';
			else
				$selectStatement .= ', ';
			$selectStatement .= $column;
		}
		return $selectStatement;
	}

	private function
	buildTablesList ($tablesParm): string
	{
		if (is_array ($tablesParm))
			$tables = implode (',' , $tablesParm );
		else
			$tables = $tablesParm;

//echo '['.$tables.']';
		return $tables;
	}

	private function
	buildFromStatement ($fromParm): string
	{
		$tables = $this -> buildTablesList($fromParm);
		if ($tables != '')
			$fromStatement = $this -> concatStrings ('FROM', $tables);
		else
			$fromStatement = '';

		return $fromStatement;
	}

	private function
	buildJoinStatements ($joinParm): string
	{
		return $this -> concatStrings ($joinParm);
	}

	private function
	buildOrderByStatement($orderParm): string
	{
		if (is_array($orderParm))
			$orderItems = $orderParm;
		else
			$orderItems = array($orderParm);

		$orderStatement = '';
		foreach ($orderItems as $orderItem)
		{
			if ($orderItem == '')
				continue;
			if ($orderStatement == "")
				$orderStatement .= " \nORDER BY ";
			else
				$orderStatement .= ", \n";
			$orderStatement .= $orderItem;
		}
		return $orderStatement;
	}

	private function
	buildGroupByStatement ($groupparm): string
	{
		if (!is_array($groupparm))
			$grouparr = array($groupparm);
		else
			$grouparr = $groupparm;

		$groupByStatement = '';
		foreach ($grouparr as $grouppart)
		{
			if ($grouppart == '')
				continue;

			if ($groupByStatement == '')
				$groupByStatement .= " \nGROUP BY ";
			else
				$groupByStatement .= ", \n";
			$groupByStatement .= $grouppart;
		}

		return $groupByStatement;
	}

	private function
	buildLimitStatement ($limitParm): string
	{
		if (is_array ($limitParm))
			$limitStatement = ' LIMIT ' . $limitParm[0] . ',' . $limitParm[1];
		else
		{
			$limitParm = trim($limitParm);
			if ($limitParm == '')
				$limitStatement = '';
			else
			{
				$limitStatement = '';
				if (strtolower (substr ($limitParm, 0, 5)) != 'limit')
					$limitStatement .= 'LIMIT ';
				$limitStatement .= $limitParm;
			}
		}
		return $limitStatement;
	}

	//--------------------------------------------------------------------
	//                            DELETE
	//--------------------------------------------------------------------

	/**
	 * Issue a DELETE request
	 * @param array parms (from, where, limit, onerror)
	 * @return bool true if success, calls errorhandler if fails
	 */
	public function
	delete ()
	{
		$num_args = func_num_args();

		$opts = [  'from', 'where', 'limit', 'onerror' ] ;

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $opts);
		else // old syntax
			$args = $this -> buildArgsForOldSyntax(func_get_args(), $opts);

		$this -> setTemporaryHandler ($args['onerror']);

		$sql = $this -> buildDeleteRequest ($args);
		if ($sql == '')
			return $this -> errorHandler('Failed to build SQL statement',1,$args['onerror']);

		$cr = $this -> executeSql ($sql);
		$this -> resetTemporaryHandler();
		return $cr;
	}

	// shorthand
	public function
	deleteOrDie ($table, $whereset)
	{
		$this -> delete ($table, $whereset, '', 'die');
	}

	// returns true if success, false if failure
	public function
	deleteSql (array $sql, string $onerror = '')
	{
		return $this -> executeSql ($sql, $onerror);
	}

	public function
	must_delete_sql (array $sql)
	{
		return $this -> delete_sql ($sql, 'die');
	}

	private function
	buildDeleteRequest (array $args): string
	{
		$fromStatement    = $this -> buildFromStatement ($args['from']);
		$whereStatement = $this -> buildWhereStatement ($args['where']);
		$limitStatement  = $this -> buildLimitStatement ($args['limit']);

		$sql = $this -> concatStrings ( 'DELETE', $fromStatement, $whereStatement , $limitStatement );

		return $sql;
	}

	//--------------------------------------------------------------------
	//                      SIMULATION AU LIEU D'EXECUTION
	//--------------------------------------------------------------------

	public function
	setReadOnly (bool $truefalse = true)
	{
		$this -> readOnly = $truefalse;
	}

	//--------------------------------------------------------------------
	//                      ENREGISTREMENT DES REQUETES
	//--------------------------------------------------------------------

	public function
	setRecording (bool $truefalse = true)
	{
		$this -> recording = true;
		$this -> resetRecorder();
	}

	public function
	doRecord (string $sql)
	{
		$this -> sqls[] = $sql;
	}

	public function
	getRecorded(): array
	{
		return $this -> sqls;
	}

	private function
	resetRecorder ()
	{
		$this -> sqls = array();
	}

	//--------------------------------------------------------------------
	//                            TRANSACTIONS
	//--------------------------------------------------------------------

	// for debug purposes
	public function
	getTransactionCount (): int
	{
		return $this -> transactionCount;
	}

	public function
	beginTransaction()
	{
		if ($this -> readOnly)
			return;

		$this -> transactionCount++;
		if ($this -> transactionCount == 1)
		{
			$this -> pdo -> beginTransaction();
			$this -> sql = 'START TRANSACTION';
		}
	}

	// alias
	public function beginTrans() { return $this -> beginTransaction(); }

	// returns true if ok, false if not
	public function
	commit(): bool
	{
tracelog ('[fwpdo::commit] commit - transactionCount='.$this->transactionCount);
		if ($this -> readOnly)
			return true;

		if ($this -> transactionCount > 1)
		{
			$this -> transactionCount--;
			return true;
		}

		if ($this -> transactionCount == 1)
		{
			$this -> sql = 'COMMIT';
			try
			{
				if (!$this -> pdo -> commit())
					return false;
tracelog ('[fwpdo::commit] commit successful');
			}
			catch (PDOException $e)
			{
				return false;
			}
			$this -> transactionCount = 0;
			return true;
		}

		// if transactionCount == 0, no transaction has been started
		return false;
	}

	public function
	rollback(): bool
	{
		if ($this -> readOnly)
			return true;

		if ($this -> transactionCount > 1)
		{
			$this -> transactionCount--;
			return true;
		}

		if ($this -> transactionCount == 1)
		{
			$this -> sql = 'ROLLBACK';
			try
			{
				$this -> pdo -> rollback();
			}
			catch (PDOException $e)
			{
				return false;
			}

			$this -> transactionCount = 0;
			return true;
		}

		// if transactionCount == 0, no transaction has been started
		return false;
	}

	//--------------------------------------------------------------------
	//                            UPDATE
	//--------------------------------------------------------------------

	/**
	 * Update one or more records
	 * Limitation: WHERE clause can only contain '=' operators
	 * @param string $table Table to update
	 * @param array $fields Array of fields (name=>value)
	 * @param array $whereset Array of name=>value to build a WHERE clause with '='
	 * @param boolean $trimall IF true, will trim fields (default: false)
	 * @return boolean True if sucessful, false is failed
	 */
	public function
	update ()
	{
		$parms = [ 'from', 'fields', 'where', 'trimall', 'onerror' ];

		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $parms);
		else // old syntax
			$args = $this -> buildArgsForOldSyntax (func_get_args(), $parms);

		$this -> setTemporaryHandler ($args['onerror']);

		$sql = $this -> buildUpdateRequest ($args);
		if ($sql == '')
			return $this -> errorHandler('Empty SQL');

		$cr = $this -> executeSql ($sql);

		$this -> resetTemporaryHandler();
		return $cr;
	}

	public function
	must_update ($table, $fields, $whereset, $trimall=false)
	{
		return $this -> update ($table, $fields, $whereset, $trimall, 'die');
	}

	/**
	 * Issue an UPDATE statement
	 * @param string $sql SQL statement
	 * @param boolean $trimall IF true, will trim fields (default: false)
	 * @return boolean True if sucessful, false is failed
	 */
	public function
	update_sql (string $sql, bool $trimall = false, string $onerror = '')
	{
		return $this -> executeSql ($sql, $onerror);
	}

	public function
	must_update_sql (string $sql, bool $trimall = false)
	{
		return $this -> update_sql ($sql, $trimall, 'die');
	}

	/**
	 * Builds an UPDATE statement
	 * @param string $table Dataabase table
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @param array $whereset Array of $fieldname=>$fieldvalue
	 * @return string SQL statement
	 */
	private function
	buildUpdateRequest ($args): string
	{
		// Create missing entries, if any

		$tables  = $this -> buildTablesList ($args['from']);
		$whereStatement = $this -> buildWhereStatement ($args['where']);
		$setStatement   = $this -> buildSetStatement ($args['fields'], $args['trimall']);

		$sql = $this -> concatStrings ( 
			'UPDATE',
			$tables,
			'SET',
			$setStatement,
			$whereStatement
			 );
		return $sql;
	}

	private function
	buildSetStatement (array $setFields, bool $trimall = false): string
	{
		$setArray = array();

		foreach ($setFields as $fieldName=>$fieldValue)
			$setArray[] =  $this->formatFieldName($fieldName) . '=' . $this->formatFieldValue($fieldValue, $trimall);

		return implode (',' , $setArray);
	}

	//--------------------------------------------------------------------
	//                            INSERT
	//--------------------------------------------------------------------

	/**
	 * Insert a single record
	 * @param string $table Database table
	 * @param array $fields Array of columns (name=>value)
	 * @param boolean $trimall Trim before insert if true (default: no)
	 * @return int ID of inserted element, 0 if failed
	 */
	public function
	insert ()
	{
		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = func_get_arg(0);
		else // old syntax
			$args = $this -> buildArgsForOldInsertSyntax(func_get_args());

		$sql = $this -> buildInsertRequest ($args);
		
		if (isset ($args['onerror']))
			$onerror = $args['onerror'];
		else
			$onerror = '';
		return $this -> insert_sql ($sql, $onerror);
	}

	private function
	buildArgsForOldInsertSyntax ($args)
	{
		$parms = array();
		$parms[] = 'from';
		$parms[] = 'fields';
		$parms[] = 'trimall';
		$parms[] = 'onerror' ;
		return $this -> buildArgsForOldSyntax($args, $parms);
	}

	public function
	must_insert (string $table, array $fields, bool $trimall=false)
	{
		return $this -> insert ($table, $fields, $trimall, 'die');
	}

	/**
	 * Insert a single record from an SQL statement
	 * @param string $sql SQL statement
	 * @return int ID of inserted element, 0 if failed
	 */
	public function
	insert_sql (string $sql, string $onerror = ''): int
	{
		if (!$this -> executeSql($sql, $onerror))
			return 0;

		return $this -> lastInsertId();
	}

	public function
	must_insert_sql (string $sql)
	{
		return $this -> insert_sql ($sql, 'die');
	}

	/**
	 * Builds and INSERT statement
	 * @param string $table Database table
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @param bool $trimall TRUE if fields should be trim'ed
	 * @return string
	 */
	private function
	buildInsertRequest ($args): string
	{
		// Create missing entries, if any

		$args = $this -> addMissing ($args, [ 'from', 'fields' ] );

		$tables  = $this -> buildTablesList ($args['from']);
		$fieldNames = $this -> buildFieldNamesList ($args['fields']);
		if (isset ($args['trimall']))
			$trimall = $args['trimall'];
		else
			$trimall = false;
		$fieldValues = $this -> buildFieldValuesList ($args['fields'], $trimall);
		$sql = $this -> concatStrings ('INSERT', 'INTO', $tables, $fieldNames, 'VALUES', $fieldValues);
		return $sql;
	}
	private function
	buildFieldNamesList (array $fields): string
	{
		$fieldNameArray = array();
		foreach ($fields as $fieldname => $fieldvalue)
			$fieldNameArray[] = $this -> formatFieldName ($fieldname);

		return '(' . implode (',' , $fieldNameArray) . ')';
	}
	private function
	buildFieldValuesList (array $fields, bool $trimall): string
	{
		$fieldValueArray = array();
		foreach ($fields as $fieldname => $fieldvalue)
			$fieldValueArray[] = $this->formatFieldValue($fieldvalue, $trimall);

		return '(' . implode(',' , $fieldValueArray) . ')';
	}

	//==================================================
	//
	//             PDO METHODS
	//
	//==================================================

	public function
	lastInsertId (): int
	{
		return $this -> pdo -> lastInsertId ();
	}

	public function
	quote ($s): string
	{
		switch (gettype($s))
		{
		case 'string':
		case 'integer':
		case 'double':
			return $this -> pdo -> quote ($s);

		case 'bool' :
		case 'boolean' :
			return ($s ? 'TRUE' : 'FALSE');

		default: 
			tracelog ('quote(): parm is expected as string, '.gettype($s).' provided: '.tracelog_dump($s));
		}
	}

	//==================================================
	//
	//             MISC UTILITIES
	//
	//==================================================

	private function
	startsWith (string $str, string $pattern, bool $case_sensitive = true): bool
	{
		if ($case_sensitive)
			return ( substr ($str, 0, strlen($pattern)) == $pattern );
		else
			return ( strcasecmp (substr ($str, 0, strlen($pattern)) , $pattern ) == 0);
	}

	// concatenate strings provided
	// args can mix strings and arrays of strings
	private function
	concatStrings (): string
	{
		$parms = func_get_args();

		$result = '';

		foreach ($parms as $parm)
		{
			if (is_array ($parm))
				$result .= implode (' ', $parm);
			else
				$result .= $parm;
			$result .= ' ';
		}

		return trim ($result);
	}

	/**
	 * Returns an array with all keys, creating missing ones if mecessary
	 * @param array Array to fill
	 * @param array entries Keys
	 * @param mixed (optional) Value to use for entries created - default value: empty string
	 * @return array Filled array
	 */
	protected function
	addMissing (array $args, array $entries, string $defaultValue = ''): array
	{
		foreach ($entries as $entry)
			if (!array_key_exists($entry, $args))
				$args[$entry] = $defaultValue;

		return $args;
	}

	// old names for compatibility only

	public function on_error ($expected_behaviour) { return $this -> onError($expected_behaviour); }
	public function get_last_error () { return $this -> getLastError(); }
	//public function datetime () { return $this -> dateTime(); }
	public function get_database () { return $this -> get_database(); }
	public function num_rows () { return $this -> numRows(); }
	public function set_charset ($charset) { return $this -> setCharset($charset); }
	public function must_delete ($table, $whereset) { return $this -> deleteOrDie($table, $whereset); }
	public function delete_sql ($sql, $onerror = '') { return $this -> deleteSql ($sql, $onerror); }
}
?>
