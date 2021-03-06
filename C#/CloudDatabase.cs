﻿using Amazon;
using Amazon.S3;
using Amazon.SimpleDB.Model;
using Attila.Files;
using NLog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Attila
{

    public enum AccessLevel { NoAccess, Depositor, Reader, Author, Editor, Designer, Manager }

    public class CloudDatabase: IDisposable
    {
      #region Private properties
      private CloudInterface aws = new CloudInterface();

      private List<string> listDomains = new List<string>();
      private List<string> listS3Buckets = new List<string>();

      private DataDocument root = null;

      #endregion

      #region Private Operations

      private bool validateDomainName(string dname)
      {
        if (String.IsNullOrEmpty(dname)) { Log.Error("Empty domain name"); return false; }
        return true;
      }

 

      #region AWS SimpleDb
      private DataDocument getDocumentByName(string name, bool preload)
      {
        try
        {
          var res = new DataDocument(this, name);
          res.replaceAllItems(aws.LoadItemAttributes(DatabaseName, name));
          if (preload) res.loadS3Data(true);
          return res;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot load document '{0}' from {1}", DatabaseName, name), ex);
          return null;
        }

      }

      private DataDocument getOrCreateDocument(string name)
      {
        var res = new DataDocument(this, name);
        List<Amazon.SimpleDB.Model.Attribute> list = null;
        try
        {
          list = aws.LoadItemAttributes(DatabaseName, name);
        }
        catch 
        {
          try
          {
            res.replaceAllItems(list);
          }
          catch (Exception ex)
          {
            Log.ErrorException(String.Format("Cannot load document attributes '{0}' from {1}", DatabaseName, name), ex);
            return null;
          }
        }
        return res;

      }

      // Need clarification
      public List<DataDocument> LoadData()
      {
        var res = new List<DataDocument>();
        try
        {
          var list = aws.SelectItemNames(DatabaseName, null);
          foreach (Item citem in list)
          {
            var item = new DataDocument(this, citem.Name);
            item.replaceAllItems(citem.Attributes);
            res.Add(item);
          }
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot load from SimpleDb '{0}'", DatabaseName), ex);
        }
        return res;
      }

      private bool getListOfDomains()
      {
        try
        {
          listDomains.Clear();
          var list = aws.GetListDomains();
          foreach (string domain in list)
          {
            listDomains.Add(domain);
          }
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException("Cannot get list of SDB domains", ex);
          return false;
        }
      }

      private bool loadRootDocument()
      {
        root = this["@root"];
        if (root == null)
        {
          Log.Error("Cannot find root document in SDB domain '{0}'", DatabaseName);
          return false;
        }
        if (root["database"] != DatabaseName)
        {
          Log.Error("Root document has invalid data: '{0}'", root["database"]);
          return false;
        }
        return true;
      }

      private bool createRootDocument(string dname)
      {
        try
        {
          root = CreateDocument("@root");
          root["database"] = dname;
          root.Save();
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format( "Cannot create root document for domain '{0}'", dname), ex);
          return false;
        }
      }

      private bool validateOrCreateSdbDomain(string dname)
      {
        DatabaseName = dname;
        if (listDomains.Contains(dname)) return true;
        try
        {
          aws.CreateSdbDomain(dname);
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot create SDB domain '{0}'", dname), ex);
          DatabaseName = "";
          return false;
        }
      }

      internal void saveDocumentToSdb(string Name, IEnumerable<Tuple<string, string>> items)
      {
        try
        {
          aws.SaveItemAttributes(DatabaseName, Name, items);
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot save items to SDB '{0}'", DatabaseName), ex);
        }
      }

      internal List<Amazon.SimpleDB.Model.Attribute> loadItemAttributes(string name)
      {
        try
        {
          return aws.LoadItemAttributes(DatabaseName, name);
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot load item attributes from SDB '{0}'", DatabaseName), ex);
          return new List<Amazon.SimpleDB.Model.Attribute>();
        }
      }

      internal void deleteSdbItem(string name)
      {
        try
        {
          aws.DeleteSdbItem(DatabaseName, name);
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format( "Cannot delete items from SDB '{0}'", DatabaseName), ex);
        }
      }

      internal void deleteSdbDomain()
      {
        try
        {
          //var list = aws.SelectItems(DatabaseName);
          //var listNames = from sdbItem in list select sdbItem.Name;
          //aws.DeleteSdbItems(DatabaseName, listNames);
          aws.DeleteSdbDomain(DatabaseName);
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format( "Cannot delete all items from SDB '{0}'", DatabaseName), ex);
        }
      }


      #endregion

      #region AWS S3

      private string makeS3BucketName(string dname)
      {
        return dname + ".attila.db";
      }

      private void listObjects()
      {
        try
        {
          var list = aws.GetListObjects(DatabaseName);
          foreach (string name in list)
          {
            Log.Info(name);
          }
        }
        catch (Exception ex)
        {
          Log.ErrorException("List of objects", ex);
        }
      }


      private bool getListOfBuckets()
      {
        try
        {
          listS3Buckets.Clear();
          listS3Buckets.AddRange(aws.GetListBuckets());
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException("Cannot get list of S3 buckets", ex);
          return false;
        }
      }

      private bool createS3Folder(string bucketName, string fname)
      {
        try
        {
          aws.CreateFolder(bucketName, fname);
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot create S3 folder {0} in bucket '{1}'", fname, bucketName), ex);
          return false;
        }
      }

      private bool createS3Folders(string dname)
      {
        var bucketName = makeS3BucketName(dname);
        if (!createS3Folder(bucketName, "system")) return false;
        if (!createS3Folder(bucketName, "data")) return false;
        return true;
      }

      private bool validateOrCreateS3Bucket(string dname)
      {
        string bname = makeS3BucketName(dname);
        if (listS3Buckets.Contains(bname)) return true;
        try
        {
          aws.CreateBucket(bname);
          listS3Buckets.Add(bname);
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format( "Cannot create S3 bucket '{0}'", dname), ex);
          return false;
        }
      }

      private bool deleteS3Bucket()
      {
        string bname = makeS3BucketName(DatabaseName);
        try
        {
          aws.DeleteBucket(bname);
          if (listS3Buckets.Contains(bname)) listS3Buckets.Remove(bname);
          return true;
        }
        catch (Exception ex)
        {
          Log.ErrorException(String.Format("Cannot delete S3 bucket '{0}'", DatabaseName), ex);
          return false;
        }
      }

      internal string readTextFromS3Bucket(string fname)
      {
        try
        {
          return aws.WriteToBucket(makeS3BucketName(DatabaseName), fname);
        }
        catch (AmazonS3Exception amazonS3Exception)
        {
          Log.ErrorException(String.Format("Cannot read data from S3 bucket '{0}:{1}'", DatabaseName, fname), amazonS3Exception);
          return null;
        }
      }

      internal void deleteS3File(string fname)
      {
        try
        {
          aws.DeleteFile(makeS3BucketName(DatabaseName), fname);
        }
        catch (AmazonS3Exception amazonS3Exception)
        {
          Log.ErrorException(String.Format( "Cannot delete S3 bucket '{0}:{1}'", DatabaseName, fname), amazonS3Exception);
        }
      }


      internal void saveItemToS3(DataItem item)
      {
        try
        {
          if (!item.IsAttachment)
          {
            // The item used to be an attachment, but not anymore. We need to delete old data from S3
            if (item.wasLoadedAsPath) aws.DeleteFile(makeS3BucketName(DatabaseName), item.internalPath);
            return;
          }
          aws.WriteToBucket(makeS3BucketName(DatabaseName), item.internalPath, item.rawBucketValue, item.MimeType);
        }
        catch (AmazonS3Exception amazonS3Exception)
        {
          Log.ErrorException(String.Format("Cannot write data to S3 bucket '{0}:{1}'", DatabaseName, item.internalPath), amazonS3Exception);
        }
      }


      #endregion

      #endregion

      #region Public Interface

      public void Dispose()
      {
        aws.Disconnect();
        listDomains.Clear();
        listS3Buckets.Clear();
        root = null;
      }

      #region Properties

      public string DatabaseName = "";

      /// <summary>
      /// Read-only. All the documents in a database.
      /// </summary>
      public List<DataDocument> AllDocuments = new List<DataDocument>();

      private static Logger logger = LogManager.GetCurrentClassLogger();
      internal Logger Log {
        get { return logger; }
        private set { logger = value;  } 
      }

      private bool isConnected = false;
      public bool IsConnected
      {
        get { return isConnected; }
      }

      private bool isOpen = false;
      public bool IsOpen
      {
        get { return isOpen; }
      }

      /// <summary>
      /// Read-only. The access control list for a database.
      /// </summary>
      public string ACL {
        get { return ""; } 
      }

      /// <summary>
      /// Read-only. The date a database was created.
      /// </summary>
      public DateTime Created {
        get { return DateTime.Now; } 
      }

      /// <summary>
      /// Read-only. The current user's access level to a database.
      /// </summary>
      public AccessLevel CurrentAccessLevel {
        get { return AccessLevel.Manager; } 
      }

      /// <summary>
      /// Read-write. Indicates whether updates to a server are delayed (batched) for better performance.
      /// </summary>
      public bool DelayUpdates { get; set; }
      public DateTime LastModified {
        get { return DateTime.Now; }  
      }

      /// <summary>
      /// Read-only. People, servers, and groups that have Manager access to a database.
      /// </summary>
      public ICollection<string> Managers {
        get { return new List<string>();  }  
      }

      /// <summary>
      /// Read-only. The percent of a database's total size that is occupied by real data (versus empty space).
      /// </summary>
      public double PercentUsed {
        get { return 100; }  
      }

      /// <summary>
      /// Read-only. The size of a database, in bytes.
      /// </summary>
      public double Size {
        get { return -1; } 
      }

      /// <summary>
      /// Read-write. The size quota of a database, in bytes.
      /// </summary>
      public long SizeQuota { get; set; }

      /// <summary>
      /// Read-Write. The title of a database.
      /// </summary>
      public string Title { get; set; }


      #endregion

      #region Connect to server
      public static RegionEndpoint GetRegion(string rtxt)
      {
        RegionEndpoint rg = null;
        switch (rtxt)
        {
          case "us-east-1": return RegionEndpoint.USEast1; 
          case "us-west-2": return RegionEndpoint.USWest2; 
          case "us-west-1": return RegionEndpoint.USWest1; 
          case "eu-west-1": return RegionEndpoint.EUWest1; 
          case "eu-central-1": return RegionEndpoint.EUCentral1; 
          case "ap-southeast-1": return RegionEndpoint.APSoutheast1; 
          case "ap-southeast-2": return RegionEndpoint.APSoutheast2;
          case "ap-northeast-1": return RegionEndpoint.APNortheast1; 
          case "sa-east-1": return RegionEndpoint.SAEast1; 
          default: return null;
        }
      } 

      public bool Connect(string key, string secret, RegionEndpoint ep)
      {
        isConnected = false;

        if (String.IsNullOrEmpty(key)) { Log.Error("Missing AWS App Key"); return false; }
        if (String.IsNullOrEmpty(secret)) { Log.Error("Missing AWS App Secret"); return false; }

        if (!aws.Connect(key, secret, ep)) return false;

        if (!getListOfDomains()) return false;
        if (!getListOfBuckets()) return false;
        Log.Debug("Connected to the AWS services");
        isConnected = true;
        return true;

      }

      public bool Connect()
      {
        string key = ConfigurationManager.AppSettings["aws_key"];
        string secret = ConfigurationManager.AppSettings["aws_secret"];
        string rtxt = ConfigurationManager.AppSettings["aws_region"];
        RegionEndpoint rg = GetRegion(rtxt);
        if (rg == null) { Log.Error("Invalid region {0}", rtxt); return false;  }
        return Connect(key, secret, rg);
      }

      public void Disconnect()
      {
        listDomains.Clear();
        isConnected = false;
      }




      #endregion

      #region Open Database
      /// <summary>
      /// Opens a database. A database must be open in order for a script to access its properties and methods.
      /// </summary>
      /// <param name="domain"></param>
      /// <returns></returns>
      public bool Open(string domain)
      {
        if (String.IsNullOrEmpty(domain)) { Log.Error("Empty database name"); return false; }
        if (!isConnected) Connect();
        if (!isConnected) return false;
        DatabaseName = domain;
        if (!loadRootDocument()) return false;
        isOpen = true;
        Log.Debug("Database {0} is open", domain);
        return true;
      }

      /// <summary>
      /// Opensa database. The database name is specified in the 'aws_domain' configuration parameter
      /// </summary>
      /// <returns></returns>
      public bool Open()
      {
        return Open(ConfigurationManager.AppSettings["aws_domain"]);
      }

      #endregion

      #region Initialize Database
      /// <summary>
      /// Creates a new database in the cloud, using the server and file name that you specify. 
      /// Because the new database is not based on a template, it's blank and does not contain any forms or views.
      /// </summary>
      /// <param name="dname"></param>
      /// <param name="openFlag">Indicates if you want to open the database. </param>
      /// <returns></returns>
      public bool Create(string dname, bool openFlag)
      {
        if (!validateDomainName(dname)) return false;
        if (!isConnected) Connect();
        if (!isConnected) return false;

        if (!validateOrCreateSdbDomain(dname)) return false;
        if (!createRootDocument(dname)) return false;
        if (!validateOrCreateS3Bucket(dname)) return false;
        if (!createS3Folders(dname)) return false;
        if (!openFlag) return true;

        Thread.Sleep(2000); // It takes time to update values
        return Open(dname);
      }


      #endregion

      #region Data Operations

      /// <summary>
      /// Creates a document in a database and returns a DataDocument object that represents the new document. 
      /// You must call Save if you want the new document to be saved to disk. 
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      public DataDocument CreateDocument(string name)
      {
        var res = new DataDocument(this, name);
        return res;
        //return getOrCreateDocument(name);
      }

      /// <summary>
      /// Get all the documents in a database.
      /// </summary>
      /// <returns></returns>
      public IEnumerable<string> GetAllDocuments()
      {
        var list = aws.SelectItemNames(DatabaseName, null);
        foreach (Item citem in list)
        {
          if (citem.Name != "@root")
          yield return citem.Name;
        }

      }

      /// <summary>
      /// Given selection criteria for a document, returns all documents in a database that meet the criteria.
      /// </summary>
      /// <param name="formula"></param>
      /// <param name="maxdocs"></param>
      public IEnumerable<DataDocument> Search(string formula, int maxdocs = 0)
      {
        var list = aws.SelectItems(DatabaseName, formula, maxdocs);
        foreach (Item citem in list)
        {
          if (citem.Name != "@root")
          {
            var doc = new DataDocument(this, citem.Name);
            doc.replaceAllItems(citem.Attributes);
            yield return doc;
          }
        }

      }


      public DataDocument this[string name]
      {
        get { return getDocumentByName(name, false); }
      }

      public DataDocument GetDocument(string name, bool preload = false)
      {
        return getDocumentByName(name, preload);
      }

      public void DeleteDocument(string name)
      {
        var doc = getDocumentByName(name, false);
        doc.Remove();
      }

      /// <summary>
      /// Permanently deletes a database from cloud.
      /// </summary>
      public void Remove()
      {
        deleteS3Bucket();
        deleteSdbDomain();
        listDomains.Clear();
        listS3Buckets.Clear();
      }


      #endregion

      #region Not implemented yet
      /// <summary>
      /// Creates an empty copy of the current database. The copy contains the design elements of the current database, an identical 
      /// access control list, and an identical title. The copy does not contain any documents and is not a replica.
      /// </summary>
      /// <param name="newDbName"></param>
      public void CreateCopy(string newDbName)
      {
      }

      /// <summary>
      /// Retrieves or creates a profile document.
      /// </summary>
      /// <param name="profileName"></param>
      /// <param name="userName"></param>
      public void GetProfileDocument(string profileName, string userName = "")
      {

      }

      /// <summary>
      /// Modifies a database access control list to provide the specified level of access to a person, group, or server.
      /// </summary>
      /// <param name="userName"></param>
      /// <param name="level"></param>
      public void GrantAccess(string userName, AccessLevel level)
      {

      }

      /// <summary>
      /// Returns a person's, group's, or server's current access level to a database.
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      public AccessLevel QueryAccess(string name)
      {
        return AccessLevel.NoAccess;
      }


      /// <summary>
      /// Removes a person, group, or server from a database access control list. This resets the access level for that person, group, or server to the Default setting for the database.
      /// </summary>
      /// <param name="userName"></param>
      public void RevokeAccess(string userName)
      {
      }

      #endregion

      #endregion


    }
}
