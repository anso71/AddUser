using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using  Microsoft.VisualBasic.FileIO;
using Agresso.ServerExtension;
using Agresso.Interface.CommonExtension;

namespace halost.AddUserFromIDM
{
    [ServerProgram("ADDIDMUS")] //Identification
    public class Standalone : ServerProgramBase  
    {
        public override void Run()
        {
            IServerDbAPI api = ServerAPI.Current.DatabaseAPI;
            string path = ServerAPI.Current.Parameters["path"];
            string filename = ServerAPI.Current.Parameters["filename"];
            string client = ServerAPI.Current.Parameters["client"];
  
            if (client == string.Empty)
                Me.StopReport("Klient ikke fylt ut");
            if (filename == string.Empty)
                Me.StopReport("Filnavn ikke fylt ut");


      
            DataTable users = new DataTable("Users");
            users = GetDataTableFromCsv(path + "/" + filename);
            foreach(DataRow row in users.Rows)
            {
                IStatement sqlAhsResource = CurrentContext.Database.CreateStatement();
                sqlAhsResource.Append("select date_to, Name, first_name, municipal, surname from ahsresources where client = @client and resource_id = @resource");
                sqlAhsResource["client"] = row["company"];
                sqlAhsResource["resource"] = row["workforceID"];
 
                DataTable ahsresourceTable = new DataTable("ahsresource");
                CurrentContext.Database.Read(sqlAhsResource, ahsresourceTable);
                if (ahsresourceTable.Rows.Count > 0)
                {
                     
                    if (!(row["email"]).Equals(""))
                    {
                        foreach (DataRow ahsrow in ahsresourceTable.Rows)
                        {
                            DataTable aaguser = new DataTable("aaguser");
                            IStatement sqlaaguser = CurrentContext.Database.CreateStatement();
                            sqlaaguser.Append("select user_id from aaguser where user_id = @username");
                            sqlaaguser["client"] = row["company"];
                            sqlaaguser["username"] = row["username"];
                            string user_id = "";

                            if (CurrentContext.Database.ReadValue(sqlaaguser, ref user_id))
                            {
                                Me.API.WriteLog("Bruker {0} finnes Oppdater?", row["username"]);
                                // finner ut om bruker har epost.
                                IStatement sqlagladdress = CurrentContext.Database.CreateStatement();
                                string email = "";
                                sqlagladdress.Append("Select e_mail from agladdress where dim_value = @workforceId and attribute_id = 'C0' and client =  @client");
                                sqlagladdress["workforceId"] = row["workforceId"];
                                sqlagladdress["client"] = row["company"];
                                CurrentContext.Database.ReadValue(sqlagladdress, ref email);
                                if (email.Equals(""))
                                {
                                    //legger inn email
                                    IStatement sqlagladdressIn = CurrentContext.Database.CreateStatement();
                                    sqlagladdressIn.Append("update agladdress set e_mail = @e_mail where dim_value= @workforceId and client= @client");
                                    sqlagladdressIn["e_mail"] = row["email"];
                                    sqlagladdressIn["workforceId"] = row["workforceId"];
                                    sqlagladdressIn["client"] = row["client"];
                                    CurrentContext.Database.Execute(sqlagladdressIn);
                                    Me.API.WriteLog("La til epost for bruker {0} med iden {1} for firma {2}", row["workforceId"], row["username"], row["company"]);
                                }
                                else
                                {
                                    if (!email.Equals(row["email"].ToString()))
                                        Me.API.WriteLog("Bruker {0} har epost {1} men har epost {2} i AD Gjelder client {3}", row["workforceId"], email, row["email"], row["company"]);
                                }

                            }
                            else
                            {
                                Me.API.WriteLog("Bruker {0} er ikke der. Legger bruker inn", row["username"]);
                                //Start på legge inn bruker
                                IStatement sqlaguserIn = CurrentContext.Database.CreateStatement();
                                sqlaguserIn.Append("insert into aaguser(alert_media, bflag, date_from, date_to, def_client, description, language, last_update, printer, priority_no, status, time_from, time_to, user_id, user_name, user_stamp)");
                                sqlaguserIn.Append(" values('M', '21', GETDATE(), @date_to, @client, @description, 'NO', GETDATE(), 'DEFAULT', 0, 'N', 0, 0, @username, @username, 'ADDUSERIDM')");
                                sqlaguserIn["client"] = row["company"];
                                sqlaguserIn["description"] = ahsrow["name"];
                                sqlaguserIn["username"] = row["username"];
                                sqlaguserIn["date_to"] = DateTime.Parse("Dec 31, 2099");
                                //legge inn bruker
                                CurrentContext.Database.Execute(sqlaguserIn);

                                //bruker link start
                                IStatement sqlacruserlinkIn = CurrentContext.Database.CreateStatement();
                                sqlacruserlinkIn.Append("insert into acruserlink (attribute_id,bflag,client,dim_value,last_update,user_id, user_stamp )");
                                sqlacruserlinkIn.Append(" values('C0', 0, @client, @workforceid, GETDATE(), @username, 'ADDUSERIDM')");
                                sqlacruserlinkIn["client"] = row["company"];
                                sqlacruserlinkIn["workforceid"] = row["workforceId"];
                                sqlacruserlinkIn["username"] = row["username"];
                                //legge inn link til bruker 
                                CurrentContext.Database.Execute(sqlacruserlinkIn);

                                //legge inn kobling til AD på bruker. Singel Sign On. Start
                                IStatement sqlaagusersecIn = CurrentContext.Database.CreateStatement();
                                sqlaagusersecIn.Append("insert into aagusersec(bflag, domain_info, last_update, user_id,user_stamp, variant)");
                                sqlaagusersecIn.Append(" values('0', 'Katalog/'+@username, getDate(), @username, 'ADDUSERIDM', '4')");
                                sqlaagusersecIn["username"] = row["username"];
                                //legger inn kobling til AD
                                CurrentContext.Database.Execute(sqlaagusersecIn);

                                //legge til description for bruker i agldecription
                                IStatement sqlagldescriptionIn = CurrentContext.Database.CreateStatement();
                                sqlagldescriptionIn.Append("insert into  agldescription (description,dim_value,attribute_id,language,client )");
                                sqlagldescriptionIn.Append(" values(@name, @username, 'GN', 'NO', @client)");
                                sqlagldescriptionIn["name"] = ahsrow["name"];
                                sqlagldescriptionIn["username"] = row["username"];
                                sqlagldescriptionIn["client"] = row["company"];
                                //legger inn decription 
                                CurrentContext.Database.Execute(sqlagldescriptionIn);
                            }

                        }
                    }
                    else
                    {
                        Me.API.WriteLog("Bruker {0} med ident {1} og i firma {2} har ikke epost/AD", row["workforceID"], row["username"], row["company"]);
                    }
                }
                else
                {
                    Me.API.WriteLog("Bruker {0} finnes ikke i firma {1} ", row["workforceID"], row["company"]);
                }
            }


            Me.API.WriteLog("Path {0}", path);

        }
        public override void End()
        {
            Me.API.WriteLog("Stopping  report {0}", Me.ReportName);
        }

        private static DataTable GetDataTableFromCsv(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { ";" });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
            
            }
            return csvData;
        }
    }
   
}
