using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Attila
{
  public class DataItem
  {

    private const int MAX_LENGTH = 1024; // Values larger than that will be stored in S3

    internal DataItem(DataDocument parent, string name)
    {
      document = parent;
      valueName = name;
    }

    internal DataItem(DataDocument parent, Amazon.SimpleDB.Model.Attribute atr)
    {
      document = parent;
      valueName = atr.Name;
      rawSdbValue = atr.Value; // can be ued only with SDB
    }

    #region Internal operations

    private DataDocument document = null;
    private dynamic internalValue = null;   // Stores actual value
    internal bool wasLoadedAsPath = false;

    // Path to the S3 object that contains item data
    internal string internalPath
    {
      get { return "data/" + Name + "/"+document.Name; }
    }

    internal void loadValue()
    {
      internalValue = document.db.readTextFromS3Bucket(internalPath);
    }

    internal void markAsLoaded()
    {
      hasChanged = false;
      isLoaded = true;
      wasLoadedAsPath = IsAttachment;
    }

    private void setMimeType(string atype)
    {
      wasLoadedAsPath = true;
      internalValue = null;
      mimeType = atype;
      isLoaded = false;
    }

    private void detectMimeType()
    {
      mimeType = "";
      if (internalValue is XElement) { mimeType = "application/xml"; return; }
      if (internalValue is JObject || internalValue is JArray) { mimeType = "application/json"; return; }
      if (internalValue is string)
      {
        if ((internalValue as string).Length >= MAX_LENGTH) mimeType = "text/plain";
      }
    }

    #endregion

    #region Raw SDB value

    private const string prefixForma = "!@{0}:{1}";
    private const string typeDateTime = "DTM";
    private const string typeInt = "INT";
    private const string typeDecimal = "DCM";
    private const string typeDouble = "DBL";
    private const string typeText = "TXT";
    private const string typeXml = "XML";
    private const string typeJson = "JSN";

    internal string rawSdbValue
    {
      // This called only when saving data to the SDB
      get
      {
        if (internalValue == null) return "";
        if (internalValue is DateTime) return String.Format(prefixForma, typeDateTime, internalValue);
        if (internalValue is Int32) return String.Format(prefixForma, typeInt, internalValue);
        if (internalValue is decimal) return String.Format(prefixForma, typeDecimal, internalValue);
        if (internalValue is double) return String.Format(prefixForma, typeDouble, internalValue);

        if (internalValue is XElement) return String.Format(prefixForma, typeXml, internalPath);
        if (internalValue is JObject || internalValue is JArray) return String.Format(prefixForma, typeJson, internalPath);
        if (internalValue is string)
        {
          if ((internalValue as string).Length < MAX_LENGTH) return internalValue; else return String.Format(prefixForma, typeText, internalPath);
        }
        return internalValue.ToString();
      }

      // This called only when loading data from SDB
      set
      {
        wasLoadedAsPath = false;
        isLoaded = true;
        mimeType = "";
        if (value.StartsWith("!@"))
        {
          string mark = value.Substring(2, 3);
          string txt = value.Substring(6, value.Length-6);
          switch (mark)
          {
            case typeDateTime: internalValue = DateTime.Parse(txt); break;
            case typeInt: internalValue = Convert.ToInt32(txt); break;
            case typeDouble: internalValue = Convert.ToDouble(txt); break;
            case typeDecimal: internalValue = Convert.ToDecimal(txt); break;

            case typeXml: setMimeType("application/xml"); break;
            case typeJson: setMimeType("application/json"); break;
            case typeText: setMimeType("text/plain"); break;

            default: throw new ApplicationException("Invalid raw data " + value);
          }
        }
        else {
          if (value.Length > MAX_LENGTH) throw new ApplicationException("Internal: data are too large");
          internalValue = value;          
        }
      }
    }

    #endregion

    #region Raw S3 data

    internal string rawBucketValue
    {
      // Prepare data to save to S3
      get {
        if (internalValue is string) return internalValue;
        return internalValue.ToString();
      }

      // Apply value from data loaded from S3
      set
      {
        switch (mimeType)
        {
          case "application/xml": internalValue = XElement.Parse(value); break;
          case "application/json": internalValue = JsonConvert.DeserializeObject(value); break;
          default: internalValue = value; break;
        }
      }
    }
    #endregion

    #region Public Interface
    // ============================================================================================

    public bool IsEmpty
    {
      get { return (internalValue == null); }
    }
    
    private bool isLoaded = false;
    /// <summary>
    /// Indicates is the item value is loaded to memory
    /// </summary>
    public bool IsLoaded
    {
      get { return isLoaded; }
    }

    internal bool hasChanged = false;
    public bool HasChanged
    {
      get { return hasChanged; }
    }

    public bool IsAttachment
    {
      get { return !String.IsNullOrEmpty(mimeType); }
    }
    
    private string mimeType = "";
    public string MimeType
    {
      get { return mimeType; }
    }

    /// <summary>
    ///  Read-Write. Indicates if an item is encrypted.
    /// </summary>
    private bool isEncrypted = false;
    public bool IsEncrypted
    {
      get { return isEncrypted; }
      set { isEncrypted = value; }
      // TODO: Implement item value encryption
    }

    /// <summary>
    /// Read-only. The name of an item.
    /// </summary>
    private string valueName = "";
    public string Name
    {
      get { return valueName; }
    }

    /// <summary>
    /// Read-Write. The value(s) that an item holds.
    /// </summary>
    public dynamic Values
    {
      get {
        if (!isLoaded) loadValue();
        return internalValue; 
      }

      set
      {
        internalValue = value;
        detectMimeType();
        hasChanged = true;
        document.wasChanged = true;
      }
    }

    #region Not implemented yet

    /// <summary>
    /// Read-Write. For a date-time item, returns a DateTime object representing the value of the item. 
    /// For items of other types, returns null.
    /// </summary>
    public DateTime DateTimeValue { get { return DateTime.Now; } }

    /// <summary>
    /// Read-only. Indicates whether or not an item is of type Authors. 
    /// An Authors item contains a list of user names, indicating people who have Author access to a particular document.
    /// </summary>
    public bool IsAuthors { get { return false; } }

    /// <summary>
    /// Read-only. Indicates if an item is a Names item. A Names item contains a list of user names.
    /// </summary>
    public bool IsNames { get { return false; } }

    /// <summary>
    /// Read-Write. Indicates if a user needs at least Editor access to modify an item.
    /// </summary>
    public bool IsProtected { get; set; }

    /// <summary>
    /// Read-Write. Indicates whether or not an item is of type Readers. 
    /// A Readers item contains a list of user names, indicating people who have Reader access to a particular document.
    /// </summary>
    public string IsReaders { get; set; }

    /// <summary>
    /// Read-Write. Indicates if an item contains a signature.
    /// </summary>
    public string IsSigned { get; set; }

    /// <summary>
    /// Read-only. The date that an item was last modified. 
    /// </summary>
    public DateTime LastModified { get { return DateTime.Now; } }

    /// <summary>
    /// Read-only. The document that contains an item.
    /// </summary>
    public DataDocument Parent { get { return document;  } }

    /// <summary>
    /// Read-only. A plain text representation of an item's value.
    /// </summary>
    public string Text { get { return ""; } }

    /// <summary>
    /// Read-only. The data type of an item.
    /// </summary>
    public string ItemType { 
      get {
        if (!String.IsNullOrEmpty(mimeType)) return mimeType;
        if (internalValue == null) return MimeType;
        if (internalValue is DateTime) return "DateTime";
        if (internalValue is Int32) return "Int";
        if (internalValue is decimal) return "Decimal";
        if (internalValue is double) return "Double";

        if (internalValue is XElement) return "text/xml";
        if (internalValue is JObject || internalValue is JArray) return "text/json";
        if (internalValue is string) return "text";
        return "Unknown";
 
      } 
    }

    /// <summary>
    /// Read-only. The size of an item's value in bytes.
    /// </summary>
    public int ValueLength { get { return -1; } }



    /// <summary>
    /// For an item that's a text list, adds a new value to the item without erasing any existing values.
    /// </summary>
    /// <param name="value"></param>
    public void AppendToTextList(string value)
    {
      // TODO: item - implement AddValue()
    }

    /// <summary>
    /// Given a value, checks if the value matches at least one of the item's values exactly.
    /// </summary>
    public bool Contains(dynamic value)
    {
      return false;
    }

    /// <summary>
    /// Copies an item to a specified document.
    /// </summary>
    /// <param name="document"></param>
    /// <param name="newName"></param>
    public void CopyItemToDocument(DataDocument document, string newName)
    {
    }

    /// <summary>
    /// Permanently deletes an item from a document.
    /// </summary>
    public void Remove()
    {
    }

    #endregion


    #endregion


  }
}
