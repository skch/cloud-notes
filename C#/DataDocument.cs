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

    /// <summary>
    /// The ID of a document that uniquely identifies a document across the database. 
    /// </summary>
    private string docId = "";
    public string Name
    {
      get { return docId; }
    }

    /// <summary>
    /// Read-only. Indicates if a Document object represents an existing document (not a delete stub). 
    /// </summary>
    public bool IsValid
    {
      get {
        if (db == null) return false;
        if (String.IsNullOrEmpty(Name)) return false;
        return true; 
      }
    }

    /// <summary>
    /// Read-only. All the items on a document. An item is any piece of data stored in a document.
    /// </summary>
    private Dictionary<string, DataItem> indexItems = new Dictionary<string, DataItem>();
    public ICollection<DataItem> Items
    {
      get { return indexItems.Values; }
    }

    /// <summary>
    /// Read-only. The names of the people who have saved a document.
    /// </summary>
    public string Authors {
      get { return ""; } 
    }

    /// <summary>
    /// Read-only. The date a document was created.
    /// </summary>
    public DateTime Created { get { return DateTime.Now; } }

    /// <summary>
    /// Read-only. Indicates if a document has been deleted or not.
    /// </summary>
    public bool IsDeleted { get { return false; } }

    /// <summary>
    /// Read-only. Indicates if a document is new. A document is new if it hasn't been saved.
    /// </summary>
    public bool IsNewNote { get { return false; } }

    /// <summary>
    /// Read-only. Indicates if a Document object is a profile document.
    /// </summary>
    public bool IsProfile { get { return false; } }

    /// <summary>
    /// Read-only. Indicates if a document contains a signature.
    /// </summary>
    public bool IsSigned { get { return false; } }

    /// <summary>
    /// Read-only. The date a document was last modified.
    /// </summary>
    public DateTime LastModified { get { return DateTime.Now; } }

    /// <summary>
    /// Read-only. The database that contains a document.
    /// </summary>
    public CloudDatabase ParentDatabase { get { return db; } }

    /// <summary>
    /// Read-only. The name of the person who created the signature, if a document is signed.
    /// </summary>
    public string Signer { get { return ""; } }

    /// <summary>
    /// Read-only. The size of a document in bytes, which includes the size of any file attachments on the document.
    /// </summary>
    public long Size { get { return -1; } }

    /// <summary>
    /// Read-only. The name of the certificate that verified a signature, if a document is signed.
    /// </summary>
    public string Verifier { get { return ""; } }

    /// <summary>
    /// Given the name of an item, returns the value of that item on a document.
    /// </summary>
    /// <param name="iname"></param>
    /// <returns></returns>
    public dynamic this[string iname]
    {
      get
      {
        DataItem item = null;
        if (indexItems.TryGetValue(iname, out item))
        {
          return item.Values;
        }
        return null;
      }

      set
      {
        ReplaceItemValue(iname, value);
      }
    }

    /// <summary>
    /// Given a name, returns the first item of the specified name belonging to the document.
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public DataItem GetItem(string name)
    {
      DataItem res = null;
      if (!indexItems.TryGetValue(name, out res)) return null;
      return res;
    }

    #region Add Item
    /// <summary>
    /// Creates a new item on a document and sets the item value.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public DataItem AppendItemValue(string name, dynamic value) {
      // TODO: What if item already exists
      var res = new DataItem(this, name) { hasChanged = true, Values = value };
      indexItems.Add(res.Name, res);
      wasChanged = true;
      return res;
    }

    public DataItem CreateItemFromFile(string name, string fname)
    {
      var text = File.ReadAllText(fname);
      return AppendItemValue(name, text);
    }

    public DataItem CreateItemFromXmlFile(string name, string fname)
    {
      var doc = XDocument.Load(fname);
      return AppendItemValue(name, doc.Root);
    }

    public DataItem CreateItemFromJsonFile(string name, string fname)
    {
      JObject jdoc = null;
      using (StreamReader reader = File.OpenText(fname))
      {
        jdoc = (JObject)JToken.ReadFrom(new JsonTextReader(reader));
      }
      return AppendItemValue(name, jdoc);
    }

    public DataItem CreateItemFromJson(string name, string jtext)
    {
      JObject jdoc = JObject.Parse(jtext);
      return AppendItemValue(name, jdoc);
    }

    #endregion

    /// <summary>
    /// Replaces all items of the specified name with one new item, which is assigned the specified value. 
    /// If the document does not contain an item with the specified name, the method creates a new item and adds it to the document.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="value"></param>
    public void ReplaceItemValue(string name, dynamic value)
    {
      DataItem item = null;
      if (!indexItems.TryGetValue(name, out item))
      {
        item = AppendItemValue(name, value);
      }
      else
      {
        item.Values = value;
      }
      wasChanged = true;
    }

    /// <summary>
    /// Saves any changes you have made to a document.
    /// </summary>
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

    /// <summary>
    /// Permanently deletes a document from a database.
    /// </summary>
    public void Remove()
    {
      deleteS3items();
      db.deleteSdbItem(Name);
      indexItems.Clear();
    }

    public override string ToString()
    {
      return String.Format("{0} ({1})", Name, indexItems.Count);
    }


    #region Not implemented yet


    /// <summary>
    /// Given a destination document, copies all of the items in the current document into the destination document. The item names are unchanged.
    /// </summary>
    /// <param name="document"></param>
    /// <param name="replace"></param>
    public void CopyAllItems(DataDocument document, bool replace = false)
    {
    }


    /// <summary>
    /// Given an item, copies it into the current document and optionally assigns the copied item a new name.
    /// </summary>
    /// <param name="newItem"></param>
    /// <param name="newName"></param>
    public void CopyItem(DataItem newItem, string newName)
    {
    }

    /// <summary>
    /// Copies a document into the specified database.
    /// </summary>
    /// <param name="database"></param>
    public void CopyToDatabase(CloudDatabase database)
    {
    }

    /// <summary>
    /// Encrypts a document in a database.
    /// </summary>
    public void Encrypt()
    {
    }

    /// <summary>
    /// Given the name of an item, indicates if that item exists on the document.
    /// </summary>
    /// <param name="itemName"></param>
    /// <returns></returns>
    public bool HasItem(string itemName)
    {
      return false;
    }

    /// <summary>
    /// Given the name of an item, deletes the item from a document.
    /// </summary>
    /// <param name="itemName"></param>
    public void RemoveItem(string itemName)
    {
    }


    /// <summary>
    /// Signs a document.
    /// </summary>
    public void Sign()
    {
    }

    #endregion



    #endregion
  }
}
