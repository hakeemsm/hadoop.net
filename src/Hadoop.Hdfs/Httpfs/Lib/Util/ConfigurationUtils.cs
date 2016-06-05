using System.Collections.Generic;
using System.IO;
using Javax.Xml.Parsers;
using Org.Apache.Hadoop.Conf;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Util
{
	/// <summary>Configuration utilities.</summary>
	public abstract class ConfigurationUtils
	{
		/// <summary>
		/// Copy configuration key/value pairs from one configuration to another if a property exists in the target, it gets
		/// replaced.
		/// </summary>
		/// <param name="source">source configuration.</param>
		/// <param name="target">target configuration.</param>
		public static void Copy(Configuration source, Configuration target)
		{
			Check.NotNull(source, "source");
			Check.NotNull(target, "target");
			foreach (KeyValuePair<string, string> entry in source)
			{
				target.Set(entry.Key, entry.Value);
			}
		}

		/// <summary>
		/// Injects configuration key/value pairs from one configuration to another if the key does not exist in the target
		/// configuration.
		/// </summary>
		/// <param name="source">source configuration.</param>
		/// <param name="target">target configuration.</param>
		public static void InjectDefaults(Configuration source, Configuration target)
		{
			Check.NotNull(source, "source");
			Check.NotNull(target, "target");
			foreach (KeyValuePair<string, string> entry in source)
			{
				if (target.Get(entry.Key) == null)
				{
					target.Set(entry.Key, entry.Value);
				}
			}
		}

		/// <summary>Returns a new ConfigurationUtils instance with all inline values resolved.
		/// 	</summary>
		/// <returns>a new ConfigurationUtils instance with all inline values resolved.</returns>
		public static Configuration Resolve(Configuration conf)
		{
			Configuration resolved = new Configuration(false);
			foreach (KeyValuePair<string, string> entry in conf)
			{
				resolved.Set(entry.Key, conf.Get(entry.Key));
			}
			return resolved;
		}

		// Canibalized from FileSystemAccess <code>Configuration.loadResource()</code>.
		/// <summary>Create a configuration from an InputStream.</summary>
		/// <remarks>
		/// Create a configuration from an InputStream.
		/// <p>
		/// ERROR canibalized from <code>Configuration.loadResource()</code>.
		/// </remarks>
		/// <param name="is">inputstream to read the configuration from.</param>
		/// <exception cref="System.IO.IOException">thrown if the configuration could not be read.
		/// 	</exception>
		public static void Load(Configuration conf, InputStream @is)
		{
			try
			{
				DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
				// ignore all comments inside the xml file
				docBuilderFactory.SetIgnoringComments(true);
				DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
				Document doc = builder.Parse(@is);
				ParseDocument(conf, doc);
			}
			catch (SAXException e)
			{
				throw new IOException(e);
			}
			catch (ParserConfigurationException e)
			{
				throw new IOException(e);
			}
		}

		// Canibalized from FileSystemAccess <code>Configuration.loadResource()</code>.
		/// <exception cref="System.IO.IOException"/>
		private static void ParseDocument(Configuration conf, Document doc)
		{
			try
			{
				Element root = doc.GetDocumentElement();
				if (!"configuration".Equals(root.GetTagName()))
				{
					throw new IOException("bad conf file: top-level element not <configuration>");
				}
				NodeList props = root.GetChildNodes();
				for (int i = 0; i < props.GetLength(); i++)
				{
					Node propNode = props.Item(i);
					if (!(propNode is Element))
					{
						continue;
					}
					Element prop = (Element)propNode;
					if (!"property".Equals(prop.GetTagName()))
					{
						throw new IOException("bad conf file: element not <property>");
					}
					NodeList fields = prop.GetChildNodes();
					string attr = null;
					string value = null;
					for (int j = 0; j < fields.GetLength(); j++)
					{
						Node fieldNode = fields.Item(j);
						if (!(fieldNode is Element))
						{
							continue;
						}
						Element field = (Element)fieldNode;
						if ("name".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							attr = ((Text)field.GetFirstChild()).GetData().Trim();
						}
						if ("value".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							value = ((Text)field.GetFirstChild()).GetData();
						}
					}
					if (attr != null && value != null)
					{
						conf.Set(attr, value);
					}
				}
			}
			catch (DOMException e)
			{
				throw new IOException(e);
			}
		}
	}
}
