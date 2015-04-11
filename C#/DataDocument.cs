using Amazon.SimpleDB.Model;
using Attila.Files;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Attila
{
  public class DataDocument
  {

    #region Internal features

    internal bool isLoaded = false;
    internal CloudDatabase db = null;
    internal bool wasChanged = false;

    internal DataDocument(CloudDatabase parent, string name)
    {
      db = parent;
      docId = name;
    }

    internal void replaceAllItems(List<Amazon.SimpleDB.Model.Attribute> list)
    {
      indexItems.Clear();
      foreach (Amazon.SimpleDB.Model.Attribute atr in list)
      {
        var item = new DataItem(this, atr);
        indexItems.Add(item.Name, item);
      }
    }

    #endregion

    #region Database operations
    internal void loadS3Data(bool force)
    {
      foreach (DataItem item in indexItems.Values)
      {
        if (!item.IsAttachment) continue; 
        if (!force && item.IsLoaded) continue;
        item.rawBucketValue = db.readTextFromS3Bucket(item.internalPath);
        item.markAsLoaded();
      }
    }

    private void deleteS3items()
    {
      foreach (DataItem item in indexItems.Values)
      {
        if (!item.IsAttachment) continue;
        db.deleteS3File(item.internalPath);
      }
    }

    #endregion

    #region Public Interface

    private string docId = "";
    public string Name
    {
      get { return docId; }
    }

    public bool IsValid
    {
      get {
        if (db == null) return false;
        if (String.IsNullOrEmpty(Name)) return false;
        return true; 
      }
    }

    private Dictionary<string, DataItem> indexItems = new Dictionary<string, DataItem>();
    public ICollection<DataItem> Items
    {
      get { return indexItems.Values; }
    }

    public dynamic this[string iname]
    {
      get
      {
        DataItem item = null;
        if (indexItems.TryGetValue(iname, out item))
        {
          return item.Value;
        }
        return null;
      }

      set
      {
        ReplaceItemValue(iname, value);
      }
    }

    public DataItem GetItem(string name)
    {
      DataItem res = null;
      if (!indexItems.TryGetValue(name, out res)) return null;
      return res;
    }

    #region Add Item
    public DataItem AddItem(string name, dynamic value) {
      // TODO: What if item already exists
      var res = new DataItem(this, name) { hasChanged = true, Value = value };
      indexItems.Add(res.Name, res);
      wasChanged = true;
      return res;
    }

    public DataItem AddItemFromFile(string name, string fname)
    {
      var text = File.ReadAllText(fname);
      return AddItem(name, text);
    }

    public DataItem AddItemFromXmlFile(string name, string fname)
    {
      var doc = XDocument.Load(fname);
      return AddItem(name, doc.Root);
    }

    public DataItem AddItemFromJsonFile(string name, string fname)
    {
      JObject jdoc = null;
      using (StreamReader reader = File.OpenText(fname))
      {
        jdoc = (JObject)JToken.ReadFrom(new JsonTextReader(reader));
      }
      return AddItem(name, jdoc);
    }

    public DataItem AddItemFromJson(string name, string jtext)
    {
      JObject jdoc = JObject.Parse(jtext);
      return AddItem(name, jdoc);
    }

    #endregion

    public void ReplaceItemValue(string name, dynamic value)
    {
      DataItem item = null;
      if (!indexItems.TryGetValue(name, out item))
      {
        item = AddItem(name, value);
      }
      else
      {
        item.Value = value;
      }
      wasChanged = true;
    }

    public void Save()
    {
      if (!wasChanged) return;
      try
      {
        // Save all items to the SDB
        db.saveDocumentToSdb(Name, getChangedSdbValues());  // use only with SDB

        // Save large item contents to S3
        foreach (DataItem item in indexItems.Values)
        {
          if (item.HasChanged) db.saveItemToS3(item);
          item.hasChanged = false;
        }
      }
      catch (Exception ex)
      {
        db.Log.ErrorException( String.Format("Cannot save document '{0}' to {1}", Name, db.DatabaseName), ex);
      }
    }

    // This method returns raw SDB values, not actual values
    private IEnumerable<Tuple<string, string>> getChangedSdbValues()
    {
      foreach (DataItem item in indexItems.Values)
      {
        if (item.HasChanged) yield return new Tuple<string, string>(item.Name, item.rawSdbValue); // only for SDB
      }
    }

    /// <summary>
    /// Discard all changes and reload document content from the database
    /// </summary>
    /// <param name="preload"></param>
    /// <returns></returns>
    public bool ReloadData(bool preload = false)
    {
      isLoaded = false;
      try
      {
        replaceAllItems(db.loadItemAttributes(Name));
        if (preload) loadS3Data(true);
        isLoaded = true;
        return true;
      }
      catch (Exception ex)
      {
        db.Log.ErrorException(String.Format("Cannot reload document '{0}' from {1}", db.DatabaseName, Name), ex);
        return false;
      }
    }

    public void Delete()
    {
      deleteS3items();
      db.deleteSdbItem(Name);
      indexItems.Clear();
    }

    public override string ToString()
    {
      return String.Format("{0} ({1})", Name, indexItems.Count);
    }



    #endregion
  }
}
