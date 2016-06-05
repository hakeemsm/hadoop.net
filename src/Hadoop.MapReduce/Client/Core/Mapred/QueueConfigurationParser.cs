using System;
using System.Collections.Generic;
using System.IO;
using Javax.Xml.Parsers;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Class for parsing mapred-queues.xml.</summary>
	/// <remarks>
	/// Class for parsing mapred-queues.xml.
	/// The format consists nesting of
	/// queues within queues - a feature called hierarchical queues.
	/// The parser expects that queues are
	/// defined within the 'queues' tag which is the top level element for
	/// XML document.
	/// Creates the complete queue hieararchy
	/// </remarks>
	internal class QueueConfigurationParser
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.QueueConfigurationParser
			));

		private bool aclsEnabled = false;

		protected internal Queue root = null;

		internal const string NameSeparator = ":";

		internal const string QueueTag = "queue";

		internal const string AclSubmitJobTag = "acl-submit-job";

		internal const string AclAdministerJobTag = "acl-administer-jobs";

		[Obsolete]
		internal const string AclsEnabledTag = "aclsEnabled";

		internal const string PropertiesTag = "properties";

		internal const string StateTag = "state";

		internal const string QueueNameTag = "name";

		internal const string QueuesTag = "queues";

		internal const string PropertyTag = "property";

		internal const string KeyTag = "key";

		internal const string ValueTag = "value";

		/// <summary>Default constructor for DeperacatedQueueConfigurationParser</summary>
		internal QueueConfigurationParser()
		{
		}

		internal QueueConfigurationParser(string confFile, bool areAclsEnabled)
		{
			//Default root.
			//xml tags for mapred-queues.xml
			// The value read from queues config file for this tag is not used at all.
			// To enable queue acls and job acls, mapreduce.cluster.acls.enabled is
			// to be set in mapred-site.xml
			aclsEnabled = areAclsEnabled;
			FilePath file = new FilePath(confFile).GetAbsoluteFile();
			if (!file.Exists())
			{
				throw new RuntimeException("Configuration file not found at " + confFile);
			}
			InputStream @in = null;
			try
			{
				@in = new BufferedInputStream(new FileInputStream(file));
				LoadFrom(@in);
			}
			catch (IOException ioe)
			{
				throw new RuntimeException(ioe);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		internal QueueConfigurationParser(InputStream xmlInput, bool areAclsEnabled)
		{
			aclsEnabled = areAclsEnabled;
			LoadFrom(xmlInput);
		}

		private void LoadFrom(InputStream xmlInput)
		{
			try
			{
				this.root = LoadResource(xmlInput);
			}
			catch (ParserConfigurationException e)
			{
				throw new RuntimeException(e);
			}
			catch (SAXException e)
			{
				throw new RuntimeException(e);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		internal virtual void SetAclsEnabled(bool aclsEnabled)
		{
			this.aclsEnabled = aclsEnabled;
		}

		internal virtual bool IsAclsEnabled()
		{
			return aclsEnabled;
		}

		internal virtual Queue GetRoot()
		{
			return root;
		}

		internal virtual void SetRoot(Queue root)
		{
			this.root = root;
		}

		/// <summary>Method to load the resource file.</summary>
		/// <remarks>
		/// Method to load the resource file.
		/// generates the root.
		/// </remarks>
		/// <param name="resourceInput">InputStream that provides the XML to parse</param>
		/// <returns/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Queue LoadResource(InputStream resourceInput)
		{
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
			//ignore all comments inside the xml file
			docBuilderFactory.SetIgnoringComments(true);
			//allow includes in the xml file
			docBuilderFactory.SetNamespaceAware(true);
			try
			{
				docBuilderFactory.SetXIncludeAware(true);
			}
			catch (NotSupportedException e)
			{
				Log.Info("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory +
					 NameSeparator + e);
			}
			DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
			Document doc = null;
			Element queuesNode = null;
			doc = builder.Parse(resourceInput);
			queuesNode = doc.GetDocumentElement();
			return this.ParseResource(queuesNode);
		}

		private Queue ParseResource(Element queuesNode)
		{
			Queue rootNode = null;
			try
			{
				if (!QueuesTag.Equals(queuesNode.GetTagName()))
				{
					Log.Info("Bad conf file: top-level element not <queues>");
					throw new RuntimeException("No queues defined ");
				}
				NamedNodeMap nmp = queuesNode.GetAttributes();
				Node acls = nmp.GetNamedItem(AclsEnabledTag);
				if (acls != null)
				{
					Log.Warn("Configuring " + AclsEnabledTag + " flag in " + QueueManager.QueueConfFileName
						 + " is not valid. " + "This tag is ignored. Configure " + MRConfig.MrAclsEnabled
						 + " in mapred-site.xml. See the " + " documentation of " + MRConfig.MrAclsEnabled
						 + ", which is used for enabling job level authorization and " + " queue level authorization."
						);
				}
				NodeList props = queuesNode.GetChildNodes();
				if (props == null || props.GetLength() <= 0)
				{
					Log.Info(" Bad configuration no queues defined ");
					throw new RuntimeException(" No queues defined ");
				}
				//We have root level nodes.
				for (int i = 0; i < props.GetLength(); i++)
				{
					Node propNode = props.Item(i);
					if (!(propNode is Element))
					{
						continue;
					}
					if (!propNode.GetNodeName().Equals(QueueTag))
					{
						Log.Info("At root level only \" queue \" tags are allowed ");
						throw new RuntimeException("Malformed xml document no queue defined ");
					}
					Element prop = (Element)propNode;
					//Add children to root.
					Queue q = CreateHierarchy(string.Empty, prop);
					if (rootNode == null)
					{
						rootNode = new Queue();
						rootNode.SetName(string.Empty);
					}
					rootNode.AddChild(q);
				}
				return rootNode;
			}
			catch (DOMException e)
			{
				Log.Info("Error parsing conf file: " + e);
				throw new RuntimeException(e);
			}
		}

		/// <param name="parent">Name of the parent queue</param>
		/// <param name="queueNode"/>
		/// <returns/>
		private Queue CreateHierarchy(string parent, Element queueNode)
		{
			if (queueNode == null)
			{
				return null;
			}
			//Name of the current queue.
			//Complete qualified queue name.
			string name = string.Empty;
			Queue newQueue = new Queue();
			IDictionary<string, AccessControlList> acls = new Dictionary<string, AccessControlList
				>();
			NodeList fields = queueNode.GetChildNodes();
			Validate(queueNode);
			IList<Element> subQueues = new AList<Element>();
			string submitKey = string.Empty;
			string adminKey = string.Empty;
			for (int j = 0; j < fields.GetLength(); j++)
			{
				Node fieldNode = fields.Item(j);
				if (!(fieldNode is Element))
				{
					continue;
				}
				Element field = (Element)fieldNode;
				if (QueueNameTag.Equals(field.GetTagName()))
				{
					string nameValue = field.GetTextContent();
					if (field.GetTextContent() == null || field.GetTextContent().Trim().Equals(string.Empty
						) || field.GetTextContent().Contains(NameSeparator))
					{
						throw new RuntimeException("Improper queue name : " + nameValue);
					}
					if (!parent.Equals(string.Empty))
					{
						name += parent + NameSeparator;
					}
					//generate the complete qualified name
					//parent.child
					name += nameValue;
					newQueue.SetName(name);
					submitKey = QueueManager.ToFullPropertyName(name, QueueACL.SubmitJob.GetAclName()
						);
					adminKey = QueueManager.ToFullPropertyName(name, QueueACL.AdministerJobs.GetAclName
						());
				}
				if (QueueTag.Equals(field.GetTagName()) && field.HasChildNodes())
				{
					subQueues.AddItem(field);
				}
				if (IsAclsEnabled())
				{
					if (AclSubmitJobTag.Equals(field.GetTagName()))
					{
						acls[submitKey] = new AccessControlList(field.GetTextContent());
					}
					if (AclAdministerJobTag.Equals(field.GetTagName()))
					{
						acls[adminKey] = new AccessControlList(field.GetTextContent());
					}
				}
				if (PropertiesTag.Equals(field.GetTagName()))
				{
					Properties properties = PopulateProperties(field);
					newQueue.SetProperties(properties);
				}
				if (StateTag.Equals(field.GetTagName()))
				{
					string state = field.GetTextContent();
					newQueue.SetState(QueueState.GetState(state));
				}
			}
			if (!acls.Contains(submitKey))
			{
				acls[submitKey] = new AccessControlList(" ");
			}
			if (!acls.Contains(adminKey))
			{
				acls[adminKey] = new AccessControlList(" ");
			}
			//Set acls
			newQueue.SetAcls(acls);
			//At this point we have the queue ready at current height level.
			//so we have parent name available.
			foreach (Element field_1 in subQueues)
			{
				newQueue.AddChild(CreateHierarchy(newQueue.GetName(), field_1));
			}
			return newQueue;
		}

		/// <summary>Populate the properties for Queue</summary>
		/// <param name="field"/>
		/// <returns/>
		private Properties PopulateProperties(Element field)
		{
			Properties props = new Properties();
			NodeList propfields = field.GetChildNodes();
			for (int i = 0; i < propfields.GetLength(); i++)
			{
				Node prop = propfields.Item(i);
				//If this node is not of type element
				//skip this.
				if (!(prop is Element))
				{
					continue;
				}
				if (PropertyTag.Equals(prop.GetNodeName()))
				{
					if (prop.HasAttributes())
					{
						NamedNodeMap nmp = prop.GetAttributes();
						if (nmp.GetNamedItem(KeyTag) != null && nmp.GetNamedItem(ValueTag) != null)
						{
							props.SetProperty(nmp.GetNamedItem(KeyTag).GetTextContent(), nmp.GetNamedItem(ValueTag
								).GetTextContent());
						}
					}
				}
			}
			return props;
		}

		/// <summary>Checks if there is NAME_TAG for queues.</summary>
		/// <remarks>
		/// Checks if there is NAME_TAG for queues.
		/// Checks if (queue has children)
		/// then it shouldnot have acls-* or state
		/// else
		/// throws an Exception.
		/// </remarks>
		/// <param name="node"/>
		private void Validate(Node node)
		{
			NodeList fields = node.GetChildNodes();
			//Check if <queue> & (<acls-*> || <state>) are not siblings
			//if yes throw an IOException.
			ICollection<string> siblings = new HashSet<string>();
			for (int i = 0; i < fields.GetLength(); i++)
			{
				if (!(fields.Item(i) is Element))
				{
					continue;
				}
				siblings.AddItem((fields.Item(i)).GetNodeName());
			}
			if (!siblings.Contains(QueueNameTag))
			{
				throw new RuntimeException(" Malformed xml formation queue name not specified ");
			}
			if (siblings.Contains(QueueTag) && (siblings.Contains(AclAdministerJobTag) || siblings
				.Contains(AclSubmitJobTag) || siblings.Contains(StateTag)))
			{
				throw new RuntimeException(" Malformed xml formation queue tag and acls " + "tags or state tags are siblings "
					);
			}
		}

		private static string GetSimpleQueueName(string fullQName)
		{
			int index = fullQName.LastIndexOf(NameSeparator);
			if (index < 0)
			{
				return fullQName;
			}
			return Sharpen.Runtime.Substring(fullQName, index + 1, fullQName.Length);
		}

		/// <summary>
		/// Construct an
		/// <see cref="Org.W3c.Dom.Element"/>
		/// for a single queue, constructing the inner
		/// queue &lt;name/&gt;, &lt;properties/&gt;, &lt;state/&gt; and the inner
		/// &lt;queue&gt; elements recursively.
		/// </summary>
		/// <param name="document"/>
		/// <param name="jqi"/>
		/// <returns/>
		internal static Element GetQueueElement(Document document, JobQueueInfo jqi)
		{
			// Queue
			Element q = document.CreateElement(QueueTag);
			// Queue-name
			Element qName = document.CreateElement(QueueNameTag);
			qName.SetTextContent(GetSimpleQueueName(jqi.GetQueueName()));
			q.AppendChild(qName);
			// Queue-properties
			Properties props = jqi.GetProperties();
			Element propsElement = document.CreateElement(PropertiesTag);
			if (props != null)
			{
				ICollection<string> propList = props.StringPropertyNames();
				foreach (string prop in propList)
				{
					Element propertyElement = document.CreateElement(PropertyTag);
					propertyElement.SetAttribute(KeyTag, prop);
					propertyElement.SetAttribute(ValueTag, (string)props[prop]);
					propsElement.AppendChild(propertyElement);
				}
			}
			q.AppendChild(propsElement);
			// Queue-state
			string queueState = jqi.GetState().GetStateName();
			if (queueState != null && !queueState.Equals(QueueState.Undefined.GetStateName()))
			{
				Element qStateElement = document.CreateElement(StateTag);
				qStateElement.SetTextContent(queueState);
				q.AppendChild(qStateElement);
			}
			// Queue-children
			IList<JobQueueInfo> children = jqi.GetChildren();
			if (children != null)
			{
				foreach (JobQueueInfo child in children)
				{
					q.AppendChild(GetQueueElement(document, child));
				}
			}
			return q;
		}
	}
}
