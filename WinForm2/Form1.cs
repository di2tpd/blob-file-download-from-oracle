using System;
using System.CodeDom.Compiler;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Windows.Forms;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Configuration;

namespace WinForm2
{
    public partial class Form1 : Form
    {
        private string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["conn_mssql"].ConnectionString;
        }
        private string GetOracleConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["conn_oracle"].ConnectionString;
        }

        private SqlConnection connection;
        private int dependencyChangeCount;

        public Form1()
        {
            InitializeComponent();
            InitializeConnection();
            SetupSqlDependency();
        }

        private void InitializeConnection()
        {
            connection = new SqlConnection(GetConnectionString());
            connection.Open();
        }

        private void SetupSqlDependency()
        {
            SqlDependency.Start(GetConnectionString());
            dependencyChangeCount = 0;
            SetupCommandDependency();
        }

        private void SetupCommandDependency()
        {
            using (var command = new SqlCommand("SELECT ATTACH_ID, REQUEST_BY FROM ERP.DBO.DOCUMENT_DOWNLOAD_QUEUE2 WHERE SELECTED = 'A'", connection))
            {
                var dependency = new SqlDependency(command);
                dependency.OnChange += OnDependencyChange;
                command.ExecuteReader().Close();
            }
        }

        private void OnDependencyChange(object sender, SqlNotificationEventArgs e)
        {
            dependencyChangeCount++;
            label1.Text = "Changed: " + dependencyChangeCount;
            SetupCommandDependency(); // Reset the dependency
        }

        private void button1_Click(object sender, EventArgs e)
        {
            try
            {
                ProcessDocuments();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void ProcessDocuments()
        {
            using (var conn = new SqlConnection(GetConnectionString()))
            {
                conn.Open();
                DataTable dataTable = ExecuteDocumentQuery(conn);
                ProcessEachDocument(dataTable);
            }
        }

        private DataTable ExecuteDocumentQuery(SqlConnection conn)
        {
            DataTable dataTable = new DataTable();
            using (var cmd = new SqlCommand("SELECT * FROM ERP.DBO.TEMP_DOCUMENT_REQUEST", conn))
            {
                dataTable.Load(cmd.ExecuteReader());
            }
            return dataTable;
        }

        private void ProcessEachDocument(DataTable dataTable)
        {
            foreach (DataRow row in dataTable.Rows)
            {
                string reqStatus = row["req_status"].ToString();
                string downloadStatus = row["download_status"].ToString();

                if (reqStatus == "A" && downloadStatus == "1")
                {
                    string path = ProcessOracleBlob(row);
                    UpdateDocumentStatus(row, path);
                }
            }
        }

        private string ProcessOracleBlob(DataRow row)
        {
            string tempDir = @"D:\Download\Sample Blob";
            using (var oraCon = new OracleConnection(GetOracleConnectionString()))
            {
                oraCon.Open();
                using (var comOra = new OracleCommand("SELECT DATA FROM BLOB_SAMPLE WHERE ID = :id", oraCon))
                {
                    comOra.Parameters.Add(new OracleParameter("id", OracleDbType.Int32)).Value = row["blob_id"];
                    using (var oracleDataReader = comOra.ExecuteReader())
                    {
                        if (oracleDataReader.Read())
                        {
                            OracleBlob oracleBlob = oracleDataReader.GetOracleBlob(0);
                            return SaveBlobToFile(oracleBlob, tempDir);
                        }
                    }
                }
            }
            return null;
        }

        private string SaveBlobToFile(OracleBlob oracleBlob, string directory)
        {
            using (var tempFileCollection = new TempFileCollection(directory, false))
            {
                string filePath = tempFileCollection.AddExtension("file", true);
                using (var fileStream = new FileStream(filePath, FileMode.Create))
                {
                    byte[] buffer = new byte[oracleBlob.Length];
                    int count = oracleBlob.Read(buffer, 0, buffer.Length);
                    fileStream.Write(buffer, 0, count);
                }
                return filePath;
            }
        }

        private void UpdateDocumentStatus(DataRow row, string filePath)
        {
            if (filePath == null) return;

            using (var conn = new SqlConnection(GetConnectionString()))
            {
                conn.Open();
                using (var cmd = new SqlCommand("UPDATE ERP.DBO.DOCUMENT_DOWNLOAD_QUEUE SET DOWNLOAD_STATUS = '1', IDENT = @Ident WHERE DOWNLOAD_STATUS = 'w' AND ATTACHED_DOCUMENT_ID = @DocumentId", conn))
                {
                    cmd.Parameters.AddWithValue("@Ident", Path.GetFileName(filePath));
                    cmd.Parameters.AddWithValue("@DocumentId", row["attached_document_id"]);
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
