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
            string standardrole = ServerAPI.Current.Parameters["role"];
  
            if (filename == string.Empty)
                Me.StopReport("Filnavn ikke fylt ut");


      
            DataTable users = new DataTable("Users");
            users = GetDataTableFromCsv(path + "/" + filename);
            foreach(DataRow row in users.Rows)
            {
                IStatement sqlacruserlinkStart = CurrentContext.Database.CreateStatement();
                sqlacruserlinkStart.Append("select user_id from acruserlink where client =@client and dim_value = @resource and attribute_id = 'C0'");
                sqlacruserlinkStart["client"] = row["company"];
                sqlacruserlinkStart["resource"] = row["workforceId"];
                string user_id1 = "";
                if (CurrentContext.Database.ReadValue(sqlacruserlinkStart, ref user_id1))
                {
                    if (row["username"].ToString().ToLower() != user_id1.ToLower())
                        row["username"] = user_id1;
                }

                IStatement sqlAhsResource = CurrentContext.Database.CreateStatement();
                sqlAhsResource.Append("select date_to, Name, first_name, municipal, surname from ahsresources where client = @client and resource_id = @resource");
                sqlAhsResource["client"] = row["company"];
                sqlAhsResource["resource"] = row["workforceID"];
 
                DataTable ahsresourceTable = new DataTable("ahsresource");
                CurrentContext.Database.Read(sqlAhsResource, ahsresourceTable);
                if (ahsresourceTable.Rows.Count > 0)
                {
                
                    if (!string.IsNullOrEmpty(row["email"].ToString()))
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
                                //Me.API.WriteLog("Bruker {0} finnes Oppdater?", row["username"]);
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
                                    sqlagladdressIn.Append("update agladdress set e_mail = @e_mail where dim_value= @workforceId and client= @client and attribute = 'C0'");
                                    sqlagladdressIn["e_mail"] = row["email"];
                                    sqlagladdressIn["workforceId"] = row["workforceId"];
                                    sqlagladdressIn["client"] = row["company"];
                                    CurrentContext.Database.Execute(sqlagladdressIn);
                                    Me.API.WriteLog("La til epost for bruker {0} med iden {1} for firma {2}", row["workforceId"], row["username"], row["company"]);
                                }
                                else
                                {
                                    if (!email.ToLower().Equals(row["email"].ToString().ToLower()))
                                        Me.API.WriteLog("Bruker {0} har epost {1} men har epost {2} i AD Gjelder client {3}", row["workforceId"], email, row["email"], row["company"]);
                                }

                                IStatement sqlagladdressGN = CurrentContext.Database.CreateStatement();
                                string emailGN = "";
                                sqlagladdressGN.Append("Select e_mail as emailGN from agladdress where dim_value = @username and attribute_id = 'GN'");
                                sqlagladdressGN["username"] = row["username"];
           
                                if (!(CurrentContext.Database.ReadValue(sqlagladdressGN, ref emailGN)))
                                {
                                    //få tak i counter
                                    Int32 address_id = 0;
                                    IStatement sqlcounter = CurrentContext.Database.CreateStatement();
                                    sqlcounter.Append("select counter from acrcounter where client = @client and column_name = 'ADDRESS_ID'");
                                    sqlcounter["client"] = row["company"];
                                    CurrentContext.Database.ReadValue(sqlcounter, ref address_id);

                                    IStatement sqlagladdressIn = CurrentContext.Database.CreateStatement();
                                    sqlagladdressIn.Append("insert into agladdress (attribute_id, address_id, client,country_code,dim_value,e_mail,last_update,user_id)");
                                    sqlagladdressIn.Append(" values ('GN',@address_id, @client,'NO',@user_id,@email,getDate(),'ADDUSERIDM')");
                                    sqlagladdressIn["user_id"] = row["username"];
                                    sqlagladdressIn["email"] = row["email"];
                                    sqlagladdressIn["client"] = "*";
                                    sqlagladdressIn["address_id"] = address_id;

                                    CurrentContext.Database.Execute(sqlagladdressIn);
                                    Me.API.WriteLog("La til epost for bruker {0} med iden {1} for firma {2}", row["workforceId"], row["username"], row["company"]);

                                    // oppdatere counter
                                    IStatement sqlcounterIn = CurrentContext.Database.CreateStatement();
                                    sqlcounterIn.Append("update acrcounter set counter = @address_id where client= @client and column_name = 'ADDRESS_ID'");
                                    sqlcounterIn["address_id"] = address_id + 1;
                                    sqlcounterIn["client"] = row["company"];
                                    CurrentContext.Database.Execute(sqlcounterIn);
                                }
                                else
                                {
                                    if (row["email"].ToString().ToLower() != emailGN.ToLower())
                                    {
                                        IStatement sqlagladdressIn = CurrentContext.Database.CreateStatement();
                                        sqlagladdressIn.Append("update agladdress set e_mail = @e_mail where dim_value= @username and attribute_id = 'GN'");
                                        sqlagladdressIn["e_mail"] = row["email"];
                                        sqlagladdressIn["username"] = row["username"];
                                        CurrentContext.Database.Execute(sqlagladdressIn);
                                        Me.API.WriteLog("Bruker {0} har epost {1} men har epost {2} i AD. Blitt oppdatert.", row["username"], emailGN, row["email"], row["company"]);
                                    }
                                }

                            }
                            else
                            {
                                //sjekk hvis bruker har et annet bruker navn
                                IStatement sqlacruserlink = CurrentContext.Database.CreateStatement();
                                sqlacruserlink.Append("select user_id from acruserlink where client =@client and dim_value = @resource and attribute_id = 'C0'");
                                sqlacruserlink["client"] = row["company"];
                                sqlacruserlink["resource"] = row["workforceId"];
                                string user_id2 = "";
                                if (CurrentContext.Database.ReadValue(sqlacruserlink, ref user_id2))
                                {
                                    Me.API.WriteLog("Bruker {0} har bruker ident {1} Sjekk om data fortsatt stemmer", row["username"], user_id2);

                                }
                                else
                                {
                                    Me.API.WriteLog("Bruker {0} er ikke der. Legger bruker inn", row["username"]);
                                            //Start på legge inn bruker
                                            IStatement sqlaguserIn = CurrentContext.Database.CreateStatement();
                                            sqlaguserIn.Append("insert into aaguser(alert_media, bflag, date_from, date_to, def_client, description, language, last_update, printer, priority_no, status, time_from, time_to, user_id, user_name, user_stamp)");
                                            sqlaguserIn.Append(" values('M', '5', GETDATE(), @date_to, @client, @description, 'NO', GETDATE(), 'DEFAULT', 7, 'N', 0, 0, @username, @username, 'ADDUSERIDM')");
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

                                    //clear description
                                    IStatement sqlagldescriptiondel = CurrentContext.Database.CreateStatement();
                                    sqlagldescriptiondel.Append("Delete from agldescription where dim_value = @username and attribute_id = 'GN' and language = 'NO' and client = @client");
                                    sqlagldescriptiondel["client"] = row["company"];
                                    sqlagldescriptiondel["username"] = row["username"];
                                    CurrentContext.Database.Execute(sqlagldescriptiondel);

                                            //legge til description for bruker i agldecription
                                            IStatement sqlagldescriptionIn = CurrentContext.Database.CreateStatement();
                                            sqlagldescriptionIn.Append("insert into  agldescription (description,dim_value,attribute_id,language,client )");
                                            sqlagldescriptionIn.Append(" values(@name, @username, 'GN', 'NO', @client)");
                                            sqlagldescriptionIn["name"] = ahsrow["name"];
                                            sqlagldescriptionIn["username"] = row["username"];
                                            sqlagldescriptionIn["client"] = row["company"];
                                            //legger inn decription 
                                            CurrentContext.Database.Execute(sqlagldescriptionIn);

                                            //få tak i counter
                                            Int32 sequence_ref = 0;
                                            IStatement sqlcounter = CurrentContext.Database.CreateStatement();
                                            sqlcounter.Append("select counter from acrcounter where client = @client and column_name = 'USER_DETAIL_ID'");
                                            sqlcounter["client"] = row["company"];
                                            CurrentContext.Database.ReadValue(sqlcounter, ref sequence_ref);

                                            //legger inn role  i aaguserdetail
                                            IStatement sqlaaguserdetailIn = CurrentContext.Database.CreateStatement();
                                            sqlaaguserdetailIn.Append("insert into aaguserdetail (bflag, client, date_from,date_to, last_update, role_id, sequence_no, sequence_ref, status, user_id, user_stamp)");
                                            sqlaaguserdetailIn.Append(" values('0', @client, getDate(),@date_to, getDate(), @role, '0', @sequence_ref, 'N', @user_id, 'ADDUSERIDM')");
                                            sqlaaguserdetailIn["client"] = row["company"];
                                            sqlaaguserdetailIn["date_to"] = DateTime.Parse("Dec 31, 2099");
                                            sqlaaguserdetailIn["role"] = standardrole;
                                            sqlaaguserdetailIn["user_id"] = row["username"];
                                            sqlaaguserdetailIn["sequence_ref"] = sequence_ref;
                                            CurrentContext.Database.Execute(sqlaaguserdetailIn);

                                            // oppdatere counter
                                            IStatement sqlcounterIn = CurrentContext.Database.CreateStatement();
                                            sqlcounterIn.Append("update acrcounter set counter = @sequence_ref where client= @client and column_name = 'USER_DETAIL_ID'");
                                            sqlcounterIn["sequence_ref"] = sequence_ref + 1;
                                            sqlcounterIn["client"] = row["company"];
                                            CurrentContext.Database.Execute(sqlcounterIn);

                                    //få tak i counter
                                    Int32 address_id = 0;
                                    IStatement sqlcounter2 = CurrentContext.Database.CreateStatement();
                                    sqlcounter2.Append("select counter from acrcounter where client = @client and column_name = 'ADDRESS_ID'");
                                    sqlcounter2["client"] = row["company"];
                                    CurrentContext.Database.ReadValue(sqlcounter2, ref address_id);

                                    IStatement sqlagladdressIn = CurrentContext.Database.CreateStatement();
                                    sqlagladdressIn.Append("insert into agladdress (attribute_id, address_id, client,country_code,dim_value,e_mail,last_update,user_id)");
                                    sqlagladdressIn.Append(" values ('GN',@address_id, @client,'NO',@user_id,@email,getDate(),'ADDUSERIDM')");
                                    sqlagladdressIn["user_id"] = row["username"];
                                    sqlagladdressIn["email"] = row["email"];
                                    sqlagladdressIn["client"] = "*";
                                    sqlagladdressIn["address_id"] = address_id;

                                    CurrentContext.Database.Execute(sqlagladdressIn);
                             

                                    // oppdatere counter
                                    IStatement sqlcounter2In = CurrentContext.Database.CreateStatement();
                                    sqlcounter2In.Append("update acrcounter set counter = @address_id where client= @client and column_name = 'ADDRESS_ID'");
                                    sqlcounter2In["address_id"] = address_id + 1;
                                    sqlcounter2In["client"] = row["company"];
                                    CurrentContext.Database.Execute(sqlcounter2In);

                                }
                            }

                        }
                    }
                   // else
                   // {
                    //    Me.API.WriteLog("Bruker {0} med ident {1} og i firma {2} har ikke epost/AD", row["workforceID"], row["username"], row["company"]);
                    //}
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
