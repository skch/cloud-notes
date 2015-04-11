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

    private const int MAX_LENGTH = 1000; // Values larger than that will be stored in S3

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
      get { return "data/" + Name; }
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
      if (internalValue is JObject) { mimeType = "application/json"; return; }
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
        if (internalValue is JObject) return String.Format(prefixForma, typeJson, internalPath);
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
          case "application/json": internalValue = JObject.Parse(value); break;
          default: internalValue = value; break;
        }
      }
    }
    #endregion

    #region Public Interface
    // ============================================================================================

    
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

    private bool isEncrypted = false;
    public bool IsEncrypted
    {
      get { return isEncrypted; }
      set { isEncrypted = value; }
      // TODO: Implement item value encryption
    }

    private string valueName = "";
    public string Name
    {
      get { return valueName; }
    }

    public dynamic Value
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


    public void AddValue(string value)
    {
      // TODO: item - implement AddValue()
    } 
    #endregion


  }
}
