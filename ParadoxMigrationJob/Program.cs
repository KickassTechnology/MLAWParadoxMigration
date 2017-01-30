using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using System.Data;
using System.Data.SqlClient;

namespace ParadoxMigrationJob
{
    class Program
    {
        static void Main(string[] args)
        {
            doMLAWMigration();   
            doRevisionMigration();
        }

        public static void doRevisionMigration()
        {
            //connection string to Paradox data
            string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=\"C:\\Inetpub\\wwwroot\\data_dump\";Extended Properties=\"Paradox 5.x\";";

            //get all the revision information
            OleDbConnection connection = new OleDbConnection(connectionString);
            string cmdText = "select * from [M-revisn]";
            OleDbCommand command = new OleDbCommand(cmdText, connection) {
                CommandType = CommandType.Text,
                CommandTimeout = 0x1770
            };


            //Fill our dataset with with the revisions
            DataSet dataSet = new DataSet();
            new OleDbDataAdapter { SelectCommand = command }.Fill(dataSet);
            connection.Close();

            //SQL Server connection string
            string str3 = "Server=mlawdb.cja22lachoyz.us-west-2.rds.amazonaws.com;Database=MLAW_MS;User Id=sa;Password=!sd2the2power!;";
            using (SqlConnection connection2 = new SqlConnection(str3))
            {
                connection2.Open();

                //Loop through all the revisions in Paradox and do stuff
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    //May or may not have dates for items so make them nullable - must do this. Default datetimes in C# have a date from 1900
                    DateTime? nullable = null;
                    DateTime? nullable2 = null;
                    DateTime? nullable3 = null;
                    

                    //The status id to set in the database. 4 stands for 'Active'
                    int ordStatusId = 4;

                    //Set our dates if we have them
                    if (row["Date Received"].ToString() != "")
                    {
                        try
                        {
                            //This is the date the order came in
                            nullable = new DateTime?(Convert.ToDateTime(row["Date Received"]));
                        }
                        catch (Exception)
                        {
                        }
                    }

                    if (row["Date Due"].ToString() != "")
                    {
                        try
                        {
                            //This is the date that it's due
                            nullable2 = new DateTime?(Convert.ToDateTime(row["Date Due"]));
                        }
                        catch (Exception)
                        {
                        }
                    }

                    if (row["Date Delievered"].ToString() != "")
                    {
                        //if it has a delivered date in Paradox, the status is complete
                        ordStatusId = 11;
                        try
                        {
                            //This is the date it was devilered to the client
                            nullable3 = new DateTime?(Convert.ToDateTime(row["Date Delievered"]));
                        }
                        catch (Exception)
                        {
                        }
                    }

                    //The dates in the Paradox database are manually entered and there's a lot of fat-fingered data.
                    //Also, revision dates are often empty although they were done years ago.
                    //The agreement was to mark it as delivered if it was more than 30 days old.
                    DateTime dtTest = DateTime.Now.AddDays(-30);
                    if (dtTest < nullable)
                    {
                        ordStatusId = 11;
                    }

                    //Find the ClientId of the client based on the MLAW_Number
                    DataSet ds = new DataSet();
                    SqlCommand cmdClientId = new SqlCommand("Get_Client_Id_By_MLAW_Number", connection2);
                    cmdClientId.Parameters.AddWithValue("@MLAW_Number", row["Mlaw No"].ToString().Trim());
                    cmdClientId.CommandType = CommandType.StoredProcedure;


                    SqlDataAdapter adpClientId = new SqlDataAdapter
                    {
                        SelectCommand = cmdClientId
                    };

                    adpClientId.Fill(ds);

                    //If we find a client_id, we're good to go. The scenario where we do not have a client id for a revision occurs when the MLAW_Number the revision has not been entered correctly.
                    if (ds.Tables[0].Rows.Count > 0)
                    {

                        int iClientId = Convert.ToInt32(ds.Tables[0].Rows[0]["Client_Id"]);

                        //Get the Pricing information from the database. Contract information is maintained by Accounting. Alot of it is missing or incorrect
                        DataSet ds2 = new DataSet();
                        SqlCommand cmdClientPrice = new SqlCommand("Get_Client_Foundation_Revision_Pricing", connection2);
                        cmdClientPrice.Parameters.AddWithValue("@Client_Id", iClientId);
                        cmdClientPrice.CommandType = CommandType.StoredProcedure;

                        SqlDataAdapter adpAmt = new SqlDataAdapter
                        {
                            SelectCommand = cmdClientPrice
                        };

                        adpAmt.Fill(ds2);

                        double Amt = -1.00;

                        if (ds2.Tables[0].Rows.Count > 0)
                        {
                            Amt = Convert.ToDouble(ds2.Tables[0].Rows[0]["Base"]);
                        }

                        //In Paradox the revision number(well, letter) was called a 'kid'
                        //If we have that, then we can create an MLAW_Number
                        if (row["kid"].ToString().Trim() != "")
                        {
                            //Send this off to the database. This stored procedure contains more logic
                            //that is used to determine thing like should this be an update or an insert?
                            SqlCommand command4 = new SqlCommand("Insert_Revision_2", connection2);
                            command4.Parameters.AddWithValue("@MLAW_Number", row["Mlaw No"].ToString().Trim());
                            command4.Parameters.AddWithValue("@Order_Status_Id", ordStatusId);
                            command4.Parameters.AddWithValue("@Revision_Letter", row["kid"].ToString().Trim());
                            command4.Parameters.AddWithValue("@Revision_Text", row["Revisions"].ToString().Trim());
                            command4.Parameters.AddWithValue("@Amt", Amt);
                            if (!nullable.HasValue || (nullable.Value.Year < 0x7c6))
                            {
                                command4.Parameters.AddWithValue("@ReceivedDate", DBNull.Value);
                            }
                            else
                            {
                                command4.Parameters.AddWithValue("@ReceivedDate", nullable);
                            }
                            if (!nullable2.HasValue || (nullable2.Value.Year < 0x7c6))
                            {
                                command4.Parameters.AddWithValue("@DueDate", DBNull.Value);
                            }
                            else
                            {
                                command4.Parameters.AddWithValue("@DueDate", nullable2);
                            }
                            if (!nullable3.HasValue || (nullable3.Value.Year < 0x7c6))
                            {
                                command4.Parameters.AddWithValue("@CompleteDate", DBNull.Value);
                            }
                            else
                            {
                                command4.Parameters.AddWithValue("@CompleteDate", nullable3);
                            }
                            command4.CommandType = CommandType.StoredProcedure;
                            command4.ExecuteNonQuery();
                        }
                    }


                    //So, there's an astonishing amount of orders in Paradox that have not been maintained. They just can't see that 
                    //sort of information in Paradox. So, if the revision does not have a received date, or it's older than 7/1/2016, it's marked as being done & invoiced.
                    
                    String str4 = "update Orders set Order_Status_Id = 11 WHERE Date_Received is null and Parent_Id is not null";
                    new SqlCommand(str4, connection2) { CommandType = CommandType.Text }.ExecuteNonQuery();
                    
                    str4 = "update Orders set Order_Status_Id = 11 WHERE Date_Received < '7/1/2016' and Parent_Id is not null";
                    new SqlCommand(str4, connection2) { CommandType = CommandType.Text }.ExecuteNonQuery();

                    str4 = "delete from Order_History where Status_Date is null";
                    new SqlCommand(str4, connection2) { CommandType = CommandType.Text }.ExecuteNonQuery();
                }
            }
        }

        public static void doMLAWMigration()
        {
            //this function updates or inserts the regular foundation orders in the database.

            //this is the connection string to the Paradox database.
            string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=\"C:\\Users\\Joe\\Desktop\\Migration\";Extended Properties=\"Paradox 5.x\";";
 
            //get the old/current order data
            OleDbConnection connection = new OleDbConnection(connectionString);
            string cmdText = "select * from [mlaw] order by Date_rec DESC ";
            OleDbCommand command = new OleDbCommand(cmdText, connection)
            {
                CommandType = CommandType.Text,
                CommandTimeout = 0x1770
            };
            DataSet dataSet = new DataSet();
            new OleDbDataAdapter { SelectCommand = command }.Fill(dataSet);
            connection.Close();

            //connection to the SQL Server database
            string str3 = "Server=mlawdb.cja22lachoyz.us-west-2.rds.amazonaws.com;Database=MLAW_MS;User Id=sa;Password=!sd2the2power!;";
            using (SqlConnection connection2 = new SqlConnection(str3))
            {
                connection2.Open();
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    //loop through our Paradox data and prepare it.
                    //yes, there are some orders that don't have order numbers!
                    if (row["Mlaw_no"].ToString().Trim() != "")
                    {
                        
                        int num13;
                        
                        //This bit is tricky. There are orders in the Paradox data that are not tied to a client by their vendor number
                        //They're called one-offs
                        //The way to identify these is to find the string of zeros where the vendor number should be.
                        string str6 = Convert.ToDouble(row["Mlaw_no"]).ToString("N3").Replace(",", "");
                        
                        string str7 = "0000";
                        if (str6.Length > 8)
                        {
                            str7 = str6.Substring(3, 4);
                        }

                        //Get the Client Id for this order
                        DataSet set2 = new DataSet();
                        SqlCommand command4 = new SqlCommand("mig_Get_Client_Id", connection2);
                        command4.Parameters.AddWithValue("@Client_Short_Name", row["CLIENT"].ToString().Trim());
                        command4.Parameters.AddWithValue("@Vendor_Number", str7);
                        command4.CommandType = CommandType.StoredProcedure;

                        SqlDataAdapter adapter2 = new SqlDataAdapter
                        {
                            SelectCommand = command4
                        };
                        adapter2.Fill(set2);

                        //get a list of all the subdivisions for this client
                        int num2 = Convert.ToInt32(set2.Tables[0].Rows[0]["Client_Id"]);
                        DataSet set3 = new DataSet();
                        command4 = new SqlCommand("Get_Client_Subdivisions", connection2);
                        command4.Parameters.AddWithValue("@Client_Id", num2);
                        command4.CommandType = CommandType.StoredProcedure;
                        adapter2 = new SqlDataAdapter
                        {
                            SelectCommand = command4
                        };
                        adapter2.Fill(set3);
                        int num3 = 0;

                        //The names of the subdivisions are all manually entered and they try to keep them consisitent, but they don't really achieve that.
                        //Hands down, the biggest issue is that one day, they enter something for a Subvision with a name like 'The Hills at Parmer' and the next
                        //day it will go in as 'The Hills @ Parmer'. So, we try to normalize that.
                        foreach (DataRow row2 in set3.Tables[0].Rows)
                        {
                            string str36 = row2["Subdivision_Name"].ToString().Trim();
                            string str37 = row["SUBDIVISION"].ToString().Trim();
                            if ((str36 == str37) || (str36.Replace("@", "at") == str37))
                            {
                                num3 = Convert.ToInt32(row2["Subdivision_Id"]);
                            }
                        }

                        //If we don't find a Subdivision in the database, we need to create it
                        //We need to parse the MLAW_Number to get the information about the order that we need.
                        if (num3 == 0)
                        {
                            int num10 = 1;
                            int num11 = 0;
                            if (str6.Length == 14)
                            {
                                num10 = Convert.ToInt32(str6.Substring(2, 1));
                            }
                            if (str6.IndexOf(".") > 0)
                            {
                                int startIndex = str6.IndexOf(".") - 2;
                                num11 = Convert.ToInt32(str6.Substring(startIndex, 2));
                            }
                            DataSet set4 = new DataSet();
                            command4 = new SqlCommand("mig_Insert_Subdivision_2", connection2);
                            command4.Parameters.AddWithValue("@Client_Id", num2);
                            command4.Parameters.AddWithValue("@Division_Id", num10);
                            command4.Parameters.AddWithValue("@Subdivision_Name", row["SUBDIVISION"].ToString());
                            command4.Parameters.AddWithValue("@Subdivision_Number", num11);
                            command4.CommandType = CommandType.StoredProcedure;
                            adapter2 = new SqlDataAdapter
                            {
                                SelectCommand = command4
                            };
                            adapter2.Fill(set4);
                            num3 = Convert.ToInt32(set2.Tables[0].Rows[0][0]);
                        }

                        //now we set the rest of the data to insert for the order
                        string str8 = row["Address"].ToString();
                        string str9 = row["City"].ToString();
                        string str10 = "TX";
                        string str11 = "";

                        //Must be nullable
                        DateTime? nullable = null;
                        DateTime? nullable2 = null;
                        DateTime? nullable3 = null;
                        DateTime? nullable4 = null;
                        DateTime? nullable5 = null;

                        //Sets the order as active
                        int num4 = 4;
                        if (row["Date_rec"].ToString() != "")
                        {
                            try
                            {
                                nullable = new DateTime?(Convert.ToDateTime(row["Date_rec"]));
                            }
                            catch (Exception)
                            {
                            }
                        }


                        if (row["Date_due"].ToString() != "")
                        {
                            try
                            {
                                nullable2 = new DateTime?(Convert.ToDateTime(row["Date_due"]));
                            }
                            catch (Exception)
                            {
                            }
                        }

                        if (row["Date_delieved"].ToString() != "")
                        {
                            //if we have a delivered date, our order status is complete
                            num4 = 10;
                            try
                            {
                                nullable3 = new DateTime?(Convert.ToDateTime(row["Date_delieved"]));
                                nullable4 = new DateTime?(Convert.ToDateTime(row["Date_delieved"]));
                            }
                            catch (Exception)
                            {
                            }
                        }
                        
                        if ((row["Date Inv"].ToString() != "") && !nullable3.HasValue)
                        {
                            //if we have an invoiced date, our Order status is Invoiced
                            num4 = 11;
                            try
                            {
                                nullable3 = new DateTime?(Convert.ToDateTime(row["Date Inv"]));
                            }
                            catch (Exception)
                            {
                            }
                        }

                        if (row["Date Inv"].ToString().Trim() != "")
                        {
                            nullable5 = new DateTime?(Convert.ToDateTime(row["Date Inv"]));
                        }

                        int? nullable6 = null;

                        //Some of the invoice numbers in Paradox contain letters or special characters
                        //Skip them if this is the case.
                        if ((row["Invoice No"].ToString().Trim() != "") && int.TryParse(row["Invoice No"].ToString(), out num13))
                        {
                            nullable6 = new int?(num13);
                        }

                        int num5 = 0;
                        int num6 = 0;
                        string str12 = row["Plan_no"].ToString();
                        string str13 = row["Lot"].ToString();
                        string str14 = row["Blk"].ToString();
                        string str15 = row["Sec_ph"].ToString();
                        string str16 = "";
                        string str17 = row["PI"].ToString();
                        DateTime? nullable7 = null;

                        if (row["Visual Geotec Date"].ToString() != "")
                        {
                            try
                            {
                                nullable7 = new DateTime?(Convert.ToDateTime(row["Visual Geotec Date"]));
                            }
                            catch (Exception)
                            {
                            }
                        }
                        string str18 = row["Soils Data Source"].ToString();
                        int num7 = 0;
                        if (row["Fill Appd"].ToString() != "")
                        {
                            try
                            {
                                num7 = Convert.ToInt32(row["Fill Appd"]);
                            }
                            catch (Exception)
                            {
                            }
                        }
                        
                        string str19 = row["Slope"].ToString();

                        //Soils data. All need to be nullable. Don't enter zeros for null data as zero is a valid value for some soils data
                        double? nullable8 = null;
                        double? nullable9 = null;
                        double? nullable10 = null;
                        double? nullable11 = null;
                        double? nullable12 = null;
                        double? nullable13 = null;
                        double? nullable14 = null;
                        if (row["Slab Sq# Ft# g"].ToString().Trim() != "")
                        {
                            nullable8 = new double?(Convert.ToDouble(row["Slab Sq# Ft# g"]));
                        }
                        if (row["Em-ctr"].ToString().Trim() != "")
                        {
                            nullable9 = new double?(Convert.ToDouble(row["Em-ctr"]));
                        }
                        if (row["Em-edg"].ToString().Trim() != "")
                        {
                            nullable10 = new double?(Convert.ToDouble(row["Em-edg"]));
                        }
                        if (row["Ym-ctr"].ToString().Trim() != "")
                        {
                            nullable11 = new double?(Convert.ToDouble(row["Ym-ctr"]));
                        }
                        if (row["Ym-edg"].ToString().Trim() != "")
                        {
                            nullable12 = new double?(Convert.ToDouble(row["Ym-edg"]));
                        }
                        if (row["Bearing capacity"].ToString().Trim() != "")
                        {
                            nullable13 = new double?(Convert.ToDouble(row["Bearing capacity"]));
                        }
                        if (row["Total Charge"].ToString().Trim() != "")
                        {
                            nullable14 = new double?(Convert.ToDouble(row["Total Charge"]));
                        }
                        else
                        {
                            double? nullable15 = 0.0;
                            if (nullable8.HasValue)
                            {
                                nullable15 = nullable8;
                            }
                            DataSet set5 = new DataSet();

                            //Get our pricing data. Maintained by Accounting
                            command4 = new SqlCommand("Get_Foundation_Order_Price", connection2);
                            command4.Parameters.AddWithValue("@Client_Id", num2);
                            command4.Parameters.AddWithValue("@Sq_Ft_Threshold", nullable15);
                            command4.CommandType = CommandType.StoredProcedure;
                            new SqlDataAdapter { SelectCommand = command4 }.Fill(set5);
                            if (set5.Tables[0].Rows[0][0].ToString() != "")
                            {
                                nullable14 = new double?(Convert.ToDouble(set5.Tables[0].Rows[0][0]));
                            }
                        }
                        string str20 = row["Comments"].ToString();
                        string str21 = "";
                        string str22 = "";
                        string str23 = "";
                        string str24 = "";
                        string str25 = "";
                        string str26 = "";
                        string str27 = "0";
                        string str28 = "";
                        string str29 = "";
                        string str30 = "";
                        string str31 = "";
                        string str32 = "";
                        int num8 = 0;
                        int num9 = 0;
                        string str33 = "";
                        string str34 = "";
                        string str35 = "";
                        
                        //Send it off the stored procedure in the database. There's more data there
                        command4 = new SqlCommand("Insert_Order_2", connection2);
                        command4.Parameters.AddWithValue("@MLAW_Number", str6);
                        command4.Parameters.AddWithValue("@MLAB_Number", "");
                        command4.Parameters.AddWithValue("@Address", str8);
                        command4.Parameters.AddWithValue("@City", str9);
                        command4.Parameters.AddWithValue("@State", str10);
                        command4.Parameters.AddWithValue("@Zip", str11);
                        command4.Parameters.AddWithValue("@Subdivision_Id", num3);
                        if (!nullable.HasValue || (nullable.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Date_Received", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Date_Received", nullable);
                        }
                        if (!nullable2.HasValue || (nullable2.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Date_Due", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Date_Due", nullable2);
                        }
                        if (!nullable4.HasValue || (nullable4.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Date_Delivered", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Date_Delivered", nullable4);
                        }
                        if (!nullable3.HasValue || (nullable3.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Date_Complete", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Date_Complete", nullable3);
                        }
                        if (!nullable5.HasValue || (nullable5.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Date_Invoiced", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Date_Invoiced", nullable);
                        }
                        command4.Parameters.AddWithValue("@Inv_Number", !nullable6.HasValue ? ((object)DBNull.Value) : ((object)nullable6));
                        command4.Parameters.AddWithValue("@Order_Status_Id", num4);
                        command4.Parameters.AddWithValue("@Order_Warranty_Id", num5);
                        command4.Parameters.AddWithValue("@Order_Type_Id", num6);
                        command4.Parameters.AddWithValue("@Plan_Number", str12);
                        command4.Parameters.AddWithValue("@Plan_Name", str16);
                        command4.Parameters.AddWithValue("@Lot", str13);
                        command4.Parameters.AddWithValue("@Block", str14);
                        command4.Parameters.AddWithValue("@Section", str15);
                        command4.Parameters.AddWithValue("@PI", str17);
                        if (!nullable7.HasValue || (nullable7.Value.Year < 0x7c6))
                        {
                            command4.Parameters.AddWithValue("@Visual_Geotec_Date", DBNull.Value);
                        }
                        else
                        {
                            command4.Parameters.AddWithValue("@Visual_Geotec_Date", nullable7);
                        }
                        command4.Parameters.AddWithValue("@Soils_Data_Source", str18);
                        command4.Parameters.AddWithValue("@Fill_Applied", num7);
                        command4.Parameters.AddWithValue("@Slope", str19);
                        command4.Parameters.AddWithValue("@Slab_Square_Feet", !nullable8.HasValue ? ((object)DBNull.Value) : ((object)nullable8));
                        command4.Parameters.AddWithValue("@Em_ctr", !nullable9.HasValue ? ((object)DBNull.Value) : ((object)nullable9));
                        command4.Parameters.AddWithValue("@Em_edg", !nullable10.HasValue ? ((object)DBNull.Value) : ((object)nullable10));
                        command4.Parameters.AddWithValue("@Ym_ctr", !nullable11.HasValue ? ((object)DBNull.Value) : ((object)nullable11));
                        command4.Parameters.AddWithValue("@Ym_edg", !nullable12.HasValue ? ((object)DBNull.Value) : ((object)nullable12));
                        command4.Parameters.AddWithValue("@Brg_cap", !nullable13.HasValue ? ((object)DBNull.Value) : ((object)nullable13));
                        command4.Parameters.AddWithValue("@Amount", !nullable14.HasValue ? ((object)DBNull.Value) : ((object)nullable14));
                        command4.Parameters.AddWithValue("@SF", num9);
                        command4.Parameters.AddWithValue("@Comments", str20);
                        command4.Parameters.AddWithValue("@Elevation", str21);
                        command4.Parameters.AddWithValue("@Contact", str22);
                        command4.Parameters.AddWithValue("@Phone", str23);
                        command4.Parameters.AddWithValue("@FoundationType", str24);
                        command4.Parameters.AddWithValue("@Phase", str25);
                        command4.Parameters.AddWithValue("@County", str26);
                        command4.Parameters.AddWithValue("@IsRevision", Convert.ToInt32(str27));
                        command4.Parameters.AddWithValue("@GarageType", str28);
                        command4.Parameters.AddWithValue("@Patio", str29);
                        command4.Parameters.AddWithValue("@Fireplace", str30);
                        command4.Parameters.AddWithValue("@Garage_Options", str31);
                        command4.Parameters.AddWithValue("@Patio_Options", str32);
                        command4.Parameters.AddWithValue("@Masonry_Sides", num8);
                        command4.Parameters.AddWithValue("@Fill_Depth", str33);
                        command4.Parameters.AddWithValue("@Soils_Comments", str34);
                        command4.Parameters.AddWithValue("@Customer_Job_Number", str35);
                        command4.CommandType = CommandType.StoredProcedure;
                        command4.ExecuteNonQuery();
                    }
                }

                //So, there's a lot of really old data that was never updated. If it was received before 1/1/2015, mark it as done.
                string str4 = "update Orders set Order_Status_Id = 10 WHERE Date_Received < '1/1/2015'";

                //Orders in Paradox have not flags for being on hold. If we see the work 'hold' in the comments, and it's marked as active, put it on hold.
                string str5 = "update Orders set Order_Status_Id = 3 WHERE Order_Status_Id = 4 AND Comments like '%hold%'";
                new SqlCommand(str4, connection2) { CommandType = CommandType.Text }.ExecuteNonQuery();
                new SqlCommand(str5, connection2) { CommandType = CommandType.Text }.ExecuteNonQuery();
                connection2.Close();
            }
        }

    }
}
