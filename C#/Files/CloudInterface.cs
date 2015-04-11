using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Attila.Files
{

  /// <summary>
  /// This class consolidates AWS SimpleDB and AWS S3 SDK interface for better depencencies management
  /// </summary>
  internal class CloudInterface
  {

    public string lastError = "";

    //-------------------------------------------------------------------------------------------
    public bool Connect(string AppKey, string Secret, RegionEndpoint region)
    {
      try
      {
        var awsc = new BasicAWSCredentials(AppKey, Secret);
        simpleDb = AWSClientFactory.CreateAmazonSimpleDBClient(awsc, region);
        s3 = AWSClientFactory.CreateAmazonS3Client(awsc, region);
        return true;
      }
      catch (Exception ex)
      {
        lastError = ex.Message;
        return false;
      }
    }

    public void Disconnect()
    {
      simpleDb.Dispose(); simpleDb = null;
      s3.Dispose(); s3 = null;
      lastError = "";
    }

    #region Aws SimpleDb
    private IAmazonSimpleDB simpleDb = null;

    //-------------------------------------------------------------------------------------------
    public List<string> GetListDomains()
    {
      ListDomainsResponse response = simpleDb.ListDomains(new ListDomainsRequest());
      return response.DomainNames;
    }

    //-------------------------------------------------------------------------------------------
    internal void CreateSdbDomain(string dname)
    {
      simpleDb.CreateDomain(new CreateDomainRequest() { DomainName = dname });
    }

    internal void DeleteSdbDomain(string dname)
    {
      simpleDb.DeleteDomain(new DeleteDomainRequest() { DomainName = dname });
    }

    //-------------------------------------------------------------------------------------------
    internal List<Amazon.SimpleDB.Model.Attribute> LoadItemAttributes(string dname, string iname)
    {
       GetAttributesResponse response = simpleDb.GetAttributes(
                  new GetAttributesRequest() { DomainName = dname, ItemName = iname });          
       return response.Attributes;
    }

    //-------------------------------------------------------------------------------------------
    public void SaveItemAttributes(string dname, string iname, IEnumerable<Tuple<string, string>> listAtr)
    {
      List<ReplaceableAttribute> listReplaceAttr = new List<ReplaceableAttribute>();
      foreach (Tuple<string, string> atr in listAtr)
      {
        ReplaceableAttribute attr = new ReplaceableAttribute() { Name = atr.Item1, Replace = true, Value = atr.Item2 };
        listReplaceAttr.Add(attr);
      }
      simpleDb.PutAttributes(new PutAttributesRequest()
      {
        Attributes = listReplaceAttr,
        DomainName = dname,
        ItemName = iname
      });
    }

    //-------------------------------------------------------------------------------------------
    public List<Amazon.SimpleDB.Model.Item> SelectItems(string dname)
    {
      SelectResponse response = simpleDb.Select(new SelectRequest() { SelectExpression = "Select * from `" + dname + "`" });
      return response.Items;
    }

    public void DeleteSdbItem(string dname, string iname)
    {
      simpleDb.DeleteAttributes(new DeleteAttributesRequest()
      {
        DomainName = dname,
        ItemName = iname
      });
    }

    public void DeleteSdbItems(string dname, IEnumerable<string> list)
    {
      BatchDeleteAttributesRequest deleteRequest = new BatchDeleteAttributesRequest();
      deleteRequest.DomainName = dname;
      foreach (var iname in list)
      {
        deleteRequest.Items.Add(new DeletableItem() { Name = iname }); 
      }
      simpleDb.BatchDeleteAttributes(deleteRequest);
    }

    #endregion

    #region Aws S3

    private IAmazonS3 s3 = null;

    //-------------------------------------------------------------------------------------------
    public IEnumerable<string> GetListBuckets()
    {
      ListBucketsResponse response = s3.ListBuckets();
      foreach (S3Bucket bucket in response.Buckets)
      {
        yield return bucket.BucketName;
      }
    }

    //-------------------------------------------------------------------------------------------
    public void CreateBucket(string bname)
    {
      s3.PutBucket(new PutBucketRequest() { BucketName = bname });
    }

    //-------------------------------------------------------------------------------------------
    public void DeleteBucket(string bname)
    {
      var response = s3.ListVersions(new ListVersionsRequest() { BucketName = bname });
      // ListObjectsResponse response = s3.ListObjects(new ListObjectsRequest() { BucketName = bname });
      if (response.Versions.Count > 0)
      {
        var vlist = new List<KeyVersion>();
        foreach (S3ObjectVersion so in response.Versions)
        {
          vlist.Add(new KeyVersion() { Key = so.Key, VersionId = so.VersionId });
        }
        
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest { BucketName = bname, Objects = vlist };
        s3.DeleteObjects(deleteObjectsRequest);
      }
      s3.DeleteBucket(new DeleteBucketRequest() { BucketName = bname });
    }


    //-------------------------------------------------------------------------------------------
    public IEnumerable<string> GetListObjects(string bname)
    {
      ListObjectsRequest request = new ListObjectsRequest();
      request.BucketName = bname;
      do
      {
        ListObjectsResponse response = s3.ListObjects(request);
        foreach (S3Object item in response.S3Objects)
        {
          yield return item.Key;
        }

        if (response.IsTruncated)
        {
          request.Marker = response.NextMarker;
        }
        else
        {
          request = null;
        }
      } while (request != null);
    }


    //-------------------------------------------------------------------------------------------
    internal void CreateFolder(string bucketName, string fname)
    {
      PutObjectRequest putObjectRequest = new PutObjectRequest
      {
        BucketName = bucketName,
        StorageClass = S3StorageClass.Standard,
        ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
        CannedACL = S3CannedACL.Private,
        Key = fname + "/",
        ContentBody = fname
      };

      s3.PutObject(putObjectRequest);
    }


    //-------------------------------------------------------------------------------------------
    internal void WriteToBucket(string bname, string fname, string data, string ctype)
    {
      PutObjectRequest putRequest1 = new PutObjectRequest
      {
        BucketName = bname,
        Key = fname,
        ContentBody = data,
        ContentType = ctype
      };
      var response1 = s3.PutObject(putRequest1);
    }


    //-------------------------------------------------------------------------------------------
    internal string WriteToBucket(string bname, string fname)
    {
      GetObjectRequest request = new GetObjectRequest() { BucketName = bname, Key = fname };
      GetObjectResponse response = s3.GetObject(request);
      StreamReader reader = new StreamReader(response.ResponseStream);
      string content = reader.ReadToEnd();
      reader.Close();
      return content;
    }

    //-------------------------------------------------------------------------------------------
    internal void DeleteFile(string bname, string fname)
    {
      DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest
      {
        BucketName = bname,
        Key = fname
      };
      s3.DeleteObject(deleteObjectRequest);
    }

    #endregion

  }
}
