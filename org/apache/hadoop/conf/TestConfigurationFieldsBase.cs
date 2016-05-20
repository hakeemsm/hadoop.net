using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>
	/// Base class for comparing fields in one or more Configuration classes
	/// against a corresponding .xml file.
	/// </summary>
	/// <remarks>
	/// Base class for comparing fields in one or more Configuration classes
	/// against a corresponding .xml file.  Usage is intended as follows:
	/// <p></p>
	/// <ol>
	/// <li> Create a subclass to TestConfigurationFieldsBase
	/// <li> Define <code>initializeMemberVariables</code> method in the
	/// subclass.  In this class, do the following:
	/// <p></p>
	/// <ol>
	/// <li> <b>Required</b> Set the variable <code>xmlFilename</code> to
	/// the appropriate xml definition file
	/// <li> <b>Required</b> Set the variable <code>configurationClasses</code>
	/// to an array of the classes which define the constants used by the
	/// code corresponding to the xml files
	/// <li> <b>Optional</b> Set <code>errorIfMissingConfigProps</code> if the
	/// subclass should throw an error in the method
	/// <code>testCompareXmlAgainstConfigurationClass</code>
	/// <li> <b>Optional</b> Set <code>errorIfMissingXmlProps</code> if the
	/// subclass should throw an error in the method
	/// <code>testCompareConfigurationClassAgainstXml</code>
	/// <li> <b>Optional</b> Instantiate and populate strings into one or
	/// more of the following variables:
	/// <br /><code>configurationPropsToSkipCompare</code>
	/// <br /><code>configurationPrefixToSkipCompare</code>
	/// <br /><code>xmlPropsToSkipCompare</code>
	/// <br /><code>xmlPrefixToSkipCompare</code>
	/// <br />
	/// in order to get comparisons clean
	/// </ol>
	/// </ol>
	/// <p></p>
	/// The tests to do class-to-file and file-to-class should automatically
	/// run.  This class (and its subclasses) are mostly not intended to be
	/// overridden, but to do a very specific form of comparison testing.
	/// </remarks>
	public abstract class TestConfigurationFieldsBase
	{
		/// <summary>Member variable for storing xml filename.</summary>
		protected internal string xmlFilename = null;

		/// <summary>Member variable for storing all related Configuration classes.</summary>
		protected internal java.lang.Class[] configurationClasses = null;

		/// <summary>Throw error during comparison if missing configuration properties.</summary>
		/// <remarks>
		/// Throw error during comparison if missing configuration properties.
		/// Intended to be set by subclass.
		/// </remarks>
		protected internal bool errorIfMissingConfigProps = false;

		/// <summary>Throw error during comparison if missing xml properties.</summary>
		/// <remarks>
		/// Throw error during comparison if missing xml properties.  Intended
		/// to be set by subclass.
		/// </remarks>
		protected internal bool errorIfMissingXmlProps = false;

		/// <summary>
		/// Set of properties to skip extracting (and thus comparing later) in
		/// extractMemberVariablesFromConfigurationFields.
		/// </summary>
		protected internal System.Collections.Generic.ICollection<string> configurationPropsToSkipCompare
			 = null;

		/// <summary>
		/// Set of property prefixes to skip extracting (and thus comparing later)
		/// in * extractMemberVariablesFromConfigurationFields.
		/// </summary>
		protected internal System.Collections.Generic.ICollection<string> configurationPrefixToSkipCompare
			 = null;

		/// <summary>
		/// Set of properties to skip extracting (and thus comparing later) in
		/// extractPropertiesFromXml.
		/// </summary>
		protected internal System.Collections.Generic.ICollection<string> xmlPropsToSkipCompare
			 = null;

		/// <summary>
		/// Set of property prefixes to skip extracting (and thus comparing later)
		/// in extractPropertiesFromXml.
		/// </summary>
		protected internal System.Collections.Generic.ICollection<string> xmlPrefixToSkipCompare
			 = null;

		/// <summary>Member variable to store Configuration variables for later comparison.</summary>
		private System.Collections.Generic.IDictionary<string, string> configurationMemberVariables
			 = null;

		/// <summary>Member variable to store XML properties for later comparison.</summary>
		private System.Collections.Generic.IDictionary<string, string> xmlKeyValueMap = null;

		/// <summary>
		/// Member variable to store Configuration variables that are not in the
		/// corresponding XML file.
		/// </summary>
		private System.Collections.Generic.ICollection<string> configurationFieldsMissingInXmlFile
			 = null;

		/// <summary>
		/// Member variable to store XML variables that are not in the
		/// corresponding Configuration class(es).
		/// </summary>
		private System.Collections.Generic.ICollection<string> xmlFieldsMissingInConfiguration
			 = null;

		/// <summary>
		/// Abstract method to be used by subclasses for initializing base
		/// members.
		/// </summary>
		public abstract void initializeMemberVariables();

		/// <summary>
		/// Utility function to extract &quot;public static final&quot; member
		/// variables from a Configuration type class.
		/// </summary>
		/// <param name="fields">The class member variables</param>
		/// <returns>HashMap containing <StringValue,MemberVariableName> entries</returns>
		private System.Collections.Generic.Dictionary<string, string> extractMemberVariablesFromConfigurationFields
			(java.lang.reflect.Field[] fields)
		{
			// Sanity Check
			if (fields == null)
			{
				return null;
			}
			System.Collections.Generic.Dictionary<string, string> retVal = new System.Collections.Generic.Dictionary
				<string, string>();
			// Setup regexp for valid properties
			string propRegex = "^[A-Za-z_-]+(\\.[A-Za-z_-]+)+$";
			java.util.regex.Pattern p = java.util.regex.Pattern.compile(propRegex);
			// Iterate through class member variables
			int totalFields = 0;
			string value;
			foreach (java.lang.reflect.Field f in fields)
			{
				// Filter out anything that isn't "public static final"
				if (!java.lang.reflect.Modifier.isStatic(f.getModifiers()) || !java.lang.reflect.Modifier
					.isPublic(f.getModifiers()) || !java.lang.reflect.Modifier.isFinal(f.getModifiers
					()))
				{
					continue;
				}
				// Filter out anything that isn't a string.  int/float are generally
				// default values
				if (!f.getType().getName().Equals("java.lang.String"))
				{
					continue;
				}
				// Convert found member into String
				try
				{
					value = (string)f.get(null);
				}
				catch (java.lang.IllegalAccessException)
				{
					continue;
				}
				// Special Case: Detect and ignore partial properties (ending in x)
				//               or file properties (ending in .xml)
				if (value.EndsWith(".xml") || value.EndsWith(".") || value.EndsWith("-"))
				{
					continue;
				}
				// Ignore known configuration props
				if (configurationPropsToSkipCompare != null)
				{
					if (configurationPropsToSkipCompare.contains(value))
					{
						continue;
					}
				}
				// Ignore known configuration prefixes
				bool skipPrefix = false;
				if (configurationPrefixToSkipCompare != null)
				{
					foreach (string cfgPrefix in configurationPrefixToSkipCompare)
					{
						if (value.StartsWith(cfgPrefix))
						{
							skipPrefix = true;
							break;
						}
					}
				}
				if (skipPrefix)
				{
					continue;
				}
				// Positive Filter: Look only for property values.  Expect it to look
				//                  something like: blah.blah2(.blah3.blah4...)
				java.util.regex.Matcher m = p.matcher(value);
				if (!m.find())
				{
					continue;
				}
				// Save member variable/value as hash
				retVal[value] = f.getName();
			}
			return retVal;
		}

		/// <summary>Pull properties and values from filename.</summary>
		/// <param name="filename">XML filename</param>
		/// <returns>HashMap containing <Property,Value> entries from XML file</returns>
		private System.Collections.Generic.Dictionary<string, string> extractPropertiesFromXml
			(string filename)
		{
			if (filename == null)
			{
				return null;
			}
			// Iterate through XML file for name/value pairs
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.setAllowNullValueProperties(true);
			conf.addResource(filename);
			System.Collections.Generic.Dictionary<string, string> retVal = new System.Collections.Generic.Dictionary
				<string, string>();
			System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<string
				, string>> kvItr = conf.GetEnumerator();
			while (kvItr.MoveNext())
			{
				System.Collections.Generic.KeyValuePair<string, string> entry = kvItr.Current;
				string key = entry.Key;
				// Ignore known xml props
				if (xmlPropsToSkipCompare != null)
				{
					if (xmlPropsToSkipCompare.contains(key))
					{
						continue;
					}
				}
				// Ignore known xml prefixes
				bool skipPrefix = false;
				if (xmlPrefixToSkipCompare != null)
				{
					foreach (string xmlPrefix in xmlPrefixToSkipCompare)
					{
						if (key.StartsWith(xmlPrefix))
						{
							skipPrefix = true;
							break;
						}
					}
				}
				if (skipPrefix)
				{
					continue;
				}
				if (conf.onlyKeyExists(key))
				{
					retVal[key] = null;
				}
				else
				{
					string value = conf.get(key);
					if (value != null)
					{
						retVal[key] = entry.Value;
					}
				}
				kvItr.remove();
			}
			return retVal;
		}

		/// <summary>Perform set difference operation on keyMap2 from keyMap1.</summary>
		/// <param name="keyMap1">The initial set</param>
		/// <param name="keyMap2">The set to subtract</param>
		/// <returns>Returns set operation keyMap1-keyMap2</returns>
		private static System.Collections.Generic.ICollection<string> compareConfigurationToXmlFields
			(System.Collections.Generic.IDictionary<string, string> keyMap1, System.Collections.Generic.IDictionary
			<string, string> keyMap2)
		{
			System.Collections.Generic.ICollection<string> retVal = new java.util.HashSet<string
				>(keyMap1.Keys);
			retVal.removeAll(keyMap2.Keys);
			return retVal;
		}

		/// <summary>
		/// Initialize the four variables corresponding the Configuration
		/// class and the XML properties file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setupTestConfigurationFields()
		{
			initializeMemberVariables();
			// Error if subclass hasn't set class members
			NUnit.Framework.Assert.IsTrue(xmlFilename != null);
			NUnit.Framework.Assert.IsTrue(configurationClasses != null);
			// Create class member/value map
			configurationMemberVariables = new System.Collections.Generic.Dictionary<string, 
				string>();
			foreach (java.lang.Class c in configurationClasses)
			{
				java.lang.reflect.Field[] fields = c.getDeclaredFields();
				System.Collections.Generic.IDictionary<string, string> memberMap = extractMemberVariablesFromConfigurationFields
					(fields);
				if (memberMap != null)
				{
					configurationMemberVariables.putAll(memberMap);
				}
			}
			// Create XML key/value map
			xmlKeyValueMap = extractPropertiesFromXml(xmlFilename);
			// Find class members not in the XML file
			configurationFieldsMissingInXmlFile = compareConfigurationToXmlFields(configurationMemberVariables
				, xmlKeyValueMap);
			// Find XML properties not in the class
			xmlFieldsMissingInConfiguration = compareConfigurationToXmlFields(xmlKeyValueMap, 
				configurationMemberVariables);
		}

		/// <summary>
		/// Compares the properties that are in the Configuration class, but not
		/// in the XML properties file.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testCompareConfigurationClassAgainstXml()
		{
			// Error if subclass hasn't set class members
			NUnit.Framework.Assert.IsTrue(xmlFilename != null);
			NUnit.Framework.Assert.IsTrue(configurationClasses != null);
			int missingXmlSize = configurationFieldsMissingInXmlFile.Count;
			foreach (java.lang.Class c in configurationClasses)
			{
				System.Console.Out.WriteLine(c);
			}
			System.Console.Out.WriteLine("  (" + configurationMemberVariables.Count + " member variables)"
				);
			System.Console.Out.WriteLine();
			System.Text.StringBuilder xmlErrorMsg = new System.Text.StringBuilder();
			foreach (java.lang.Class c_1 in configurationClasses)
			{
				xmlErrorMsg.Append(c_1);
				xmlErrorMsg.Append(" ");
			}
			xmlErrorMsg.Append("has ");
			xmlErrorMsg.Append(missingXmlSize);
			xmlErrorMsg.Append(" variables missing in ");
			xmlErrorMsg.Append(xmlFilename);
			System.Console.Out.WriteLine(xmlErrorMsg.ToString());
			System.Console.Out.WriteLine();
			if (missingXmlSize == 0)
			{
				System.Console.Out.WriteLine("  (None)");
			}
			else
			{
				foreach (string missingField in configurationFieldsMissingInXmlFile)
				{
					System.Console.Out.WriteLine("  " + missingField);
				}
			}
			System.Console.Out.WriteLine();
			System.Console.Out.WriteLine("=====");
			System.Console.Out.WriteLine();
			if (errorIfMissingXmlProps)
			{
				NUnit.Framework.Assert.IsTrue(xmlErrorMsg.ToString(), missingXmlSize == 0);
			}
		}

		/// <summary>
		/// Compares the properties that are in the XML properties file, but not
		/// in the Configuration class.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testCompareXmlAgainstConfigurationClass()
		{
			// Error if subclass hasn't set class members
			NUnit.Framework.Assert.IsTrue(xmlFilename != null);
			NUnit.Framework.Assert.IsTrue(configurationClasses != null);
			int missingConfigSize = xmlFieldsMissingInConfiguration.Count;
			System.Console.Out.WriteLine("File " + xmlFilename + " (" + xmlKeyValueMap.Count 
				+ " properties)");
			System.Console.Out.WriteLine();
			System.Text.StringBuilder configErrorMsg = new System.Text.StringBuilder();
			configErrorMsg.Append(xmlFilename);
			configErrorMsg.Append(" has ");
			configErrorMsg.Append(missingConfigSize);
			configErrorMsg.Append(" properties missing in");
			foreach (java.lang.Class c in configurationClasses)
			{
				configErrorMsg.Append("  " + c);
			}
			System.Console.Out.WriteLine(configErrorMsg.ToString());
			System.Console.Out.WriteLine();
			if (missingConfigSize == 0)
			{
				System.Console.Out.WriteLine("  (None)");
			}
			else
			{
				foreach (string missingField in xmlFieldsMissingInConfiguration)
				{
					System.Console.Out.WriteLine("  " + missingField);
				}
			}
			System.Console.Out.WriteLine();
			System.Console.Out.WriteLine("=====");
			System.Console.Out.WriteLine();
			if (errorIfMissingConfigProps)
			{
				NUnit.Framework.Assert.IsTrue(configErrorMsg.ToString(), missingConfigSize == 0);
			}
		}
	}
}
