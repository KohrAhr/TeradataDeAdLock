using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Xml.Linq;
using Teradata.Client.Provider;

// .Net Core 3.1 LTS
// Teradata 16.20 Express
// Teradata .Net Drivers 17.10

/*
    CREATE SET TABLE DEV.STORAGE1 ,FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
    Id INT NOT NULL,
     Name CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC)
    PRIMARY INDEX ( Id );
*/

namespace TeradataDeAdL0ckHandler
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        // We don't care about MVVC or any other pattern at all, this project is dedicated only to demonstrate how to handle DEAD LOCK in Teradata DBMS
        public MainWindow()
        {
            InitializeComponent();

            DataContext = this;
        }

        private void Log(string aValue)
        {
            Dispatcher.BeginInvoke(new Action(() =>
            {
                txtLog.Text += DateTime.Now.ToString("HH:mm:ss.fffff") + "\t" + aValue + "\n";
            }));
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            // :)
            (sender as Button).IsEnabled = txtServer.IsEnabled = false;
            Log("It's a show time!");

            //
            TdConnection tdConnection1 = new TdConnection();
            TdConnection tdConnection2 = new TdConnection();
            TdConnection tdConnection3 = new TdConnection();

            TdConnectionStringBuilder connectionStringBuilder = new TdConnectionStringBuilder();
            connectionStringBuilder.UserId = "dbc";
            connectionStringBuilder.Password = "dbc";
            connectionStringBuilder.Database = "DEV";
            // connectionStringBuilder.ConnectionTimeout = 30;
            // Not big one
            // connectionStringBuilder.CommandTimeout = 10;
            connectionStringBuilder.ConnectionPooling = true;
            // BEGIN TRANSACTION not allowed for an ANSI session.
            connectionStringBuilder.SessionMode = "TERADATA";
            connectionStringBuilder.SessionCharacterSet = "UTF8";
            connectionStringBuilder.DataSource = txtServer.Text;

            tdConnection1.ConnectionString = connectionStringBuilder.ConnectionString;
            tdConnection2.ConnectionString = connectionStringBuilder.ConnectionString;
            tdConnection3.ConnectionString = connectionStringBuilder.ConnectionString;

            //          const string CONST_SQL4 = "LOCK TABLE DEV.STORAGE1 FOR WRITE NOWAIT UPDATE DEV.STORAGE1 SET NAME = 'AZ!';";
            //            const string CONST_SQL3 = "LOCKING ROW FOR ACCESS NOWAIT SELECT COUNT(*) AS E1 FROM DEV.STORAGE1 WHERE (ID > 100);";
            const string CONST_SQL3 = "LOCK ROW FOR ACCESS NOWAIT SELECT COUNT(*) AS E1 FROM DEV.STORAGE1 WHERE (ID > 100);";


            const string CONST_SQL1 = "LOCK TABLE DEV.STORAGE1 FOR WRITE NOWAIT DELETE FROM DEV.STORAGE1 WHERE (ID > 100);";
            const string CONST_SQL2 = "INSERT INTO DEV.STORAGE1(Id, Name) VALUES({0}, '{0}');";
            const string CONST_SQL5 = "UPDATE DEV.STORAGE1 SET NAME = 'AZ!';";

            // Ok. Run command 1
            Task.Run(() =>
            {
                Log("1st Connections ...");
                tdConnection1.ConnectionString = connectionStringBuilder.ConnectionString;
                tdConnection1.Open();
                Log("1st Connections with server established");

                try
                {
                    Log("Thread 1 run: " + CONST_SQL1);
                    int result = RerunableRunSqlQuery(tdConnection1, false, CONST_SQL1, 1);
                    Log("Thread 1 result is: " + result.ToString());
                }
                catch (Exception e)
                {
                    Log(e.ToString());
                }
            });

            // Ok. Run command 2
            Task.Run(() =>
            {
                Log("2nd Connections ...");
                tdConnection2.ConnectionString = connectionStringBuilder.ConnectionString;
                tdConnection2.Open();
                Log("2nd Connections with server established");

                try
                {
                    Log("Thread 2 run 100 times: " + CONST_SQL2);
                    for (int i = 0; i < 100; i++)
                    {
                        int result = RerunableRunSqlQuery(tdConnection2, false, String.Format(CONST_SQL2, i + 1), 2);
                        Log("Thread 2 result is: " + result.ToString());
                    }
                }
                catch (Exception e)
                { 
                    Log(e.ToString());
                }
            });

            // Ok. Run command 3
            Task.Run(() =>
            {
                Log("3rd Connections ...");
                tdConnection3.ConnectionString = connectionStringBuilder.ConnectionString;
                tdConnection3.Open();
                Log("3rd Connections with server established");

                try
                {
                    Log("Thread 3 run: " + CONST_SQL5);
                    int result = RerunableRunSqlQuery(tdConnection3, true, CONST_SQL5, 2);
                    Log("Thread 3 result is: " + result.ToString());
                }
                catch (Exception e)
                {
                    Log(e.ToString());
                }
            });
        }

        // 1st attempt we always make. 
        // So, 2 mean that we handle error only once. This is not 2 additional attempt to first one, it's two attempt at all. 
        // So, 6 mean that we handle error five times. First obligatory run plus 5 extra attempts.
        // IMHO we should have not less than 3 attempts and not more than 6 for sure
        public static int CONST_MAX_ATTEMPTS = 3;

        /*
          Low level handler for errors
        */
        private int ErrorHandler(int aErrorCounter, TdException aE, int aThreadId)
        {
            // Counter of errors.
            aErrorCounter++;

            Log(String.Format("Thread: {0}. Error message is: {1}", aThreadId, aE.Message));

            // Did we make already enought attempts to re-run?
            if (aErrorCounter >= CONST_MAX_ATTEMPTS)
            {
                /* Bye bye */
                throw new TdException("ErrorHandler", aE);
            }

            // Delays in seconds on attempt; Formula;
            // 1  4   9   16  25  = 55 sec total all attempts.  x * x
            // 4  9   16  25  36  = 90 sec total all attempts.  (x + 1) * (x + 1)
            // 2  6   12  20  30  = 70 sec total all attempts.  (x + 1) * x
            // 3  8   15  24  35  = 85 sec total all attempts.  (x + 2) * x

            // Delay Formula: (x + 1) * (x + 1)
            // 4 sec after 1st failure
            // 9 sec after 2nd failure
            // 16 sec after 3rd failure

            // Use Formula you like to calculate delay interval on this iteration step
            int delay = (aErrorCounter + 1) * aErrorCounter;

            // Funny sleep timer
            Log(String.Format("Thread: {0}. Delay in seconds before another attempt is: {1}", aThreadId, delay));
            Log(String.Format("Thread: {0}. Sleep", aThreadId));
            for (int i = 0;i<delay;i++)
            {
                Thread.Sleep(1000);
                Log(String.Format("Thread: {0}. {1}", aThreadId, i));
            }

            return aErrorCounter;
        }

        /*
          Top level handler for errors
        */
        public int HandleSqlException(int aErrorCounter, TdException aE, int aThreadId) 
        {
            // Ok, at this moment we are retrying all errors, but if it's Uid/Pwd incorrect, than we don't have to retry and we should fail immidiately

            // Just add error codes into the list :)
            int[] codes =
            {
                // [.NET Data Provider for Teradata] [100038] Command did not complete within the time specified (timeout).
                100038,
                // [Socket Transport][115003] The receive operation timed out. 
                115003,
                // [Teradata Database] [7423] Object already locked and NOWAIT. Transaction Aborted.
                7423,
                // DeAd L0CK :)
                2631
            };

            int errorCode = aE.Errors[0].Number;

            // 1st type of error -- from error code and no minor code -- we have to retry
            if (codes.Contains(errorCode)/* && sqlError.equals("")*/)
            {
                aErrorCounter = ErrorHandler(aErrorCounter, aE, aThreadId);
            }
            else
            {
                throw aE;
            }

            return aErrorCounter;
        }

        public int RerunableRunSqlQuery(TdConnection aConnection, bool aRunInTransaction, string aSql, int aThreadId)
        {
            // reset error counter and Completed status
            int errorCounter = 0;
            bool completed = false;

            int result = 0;

            if (aRunInTransaction)
            {
                aConnection.BeginTransaction();
            }

            do
            {
                try
                {
                    // Who cares? Really? This is a demo!
                    // Yes, outside try (...) and without GC
                    TdCommand tdCommand = aConnection.CreateCommand();

                    tdCommand.CommandText = aSql;
                    // Error will occure in next line
                    result = tdCommand.ExecuteNonQuery();

                    // Permission to leave from Do-While block
                    completed = true;
                }
                // Swallow SQL server connection error. Handle it on special way.
                catch (TdException e)
                {
                    errorCounter = HandleSqlException(errorCounter, e, aThreadId);
                }

            }
            /*
                Continue until:
                1) we didn't hit maximum attempts
                2) connection cannot be established because of the exception
            */
            while (errorCounter < CONST_MAX_ATTEMPTS && !completed);

            return result;
        }
    }
}