using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestDefaultStringifier : NUnit.Framework.TestCase
	{
		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestDefaultStringifier
			)));

		private char[] alphabet = "abcdefghijklmnopqrstuvwxyz".ToCharArray();

		/// <exception cref="System.Exception"/>
		public virtual void testWithWritable()
		{
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization"
				);
			LOG.info("Testing DefaultStringifier with Text");
			java.util.Random random = new java.util.Random();
			//test with a Text
			for (int i = 0; i < 10; i++)
			{
				//generate a random string
				java.lang.StringBuilder builder = new java.lang.StringBuilder();
				int strLen = random.nextInt(40);
				for (int j = 0; j < strLen; j++)
				{
					builder.Append(alphabet[random.nextInt(alphabet.Length)]);
				}
				org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text(builder.ToString()
					);
				org.apache.hadoop.io.DefaultStringifier<org.apache.hadoop.io.Text> stringifier = 
					new org.apache.hadoop.io.DefaultStringifier<org.apache.hadoop.io.Text>(conf, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)));
				string str = stringifier.toString(text);
				org.apache.hadoop.io.Text claimedText = stringifier.fromString(str);
				LOG.info("Object: " + text);
				LOG.info("String representation of the object: " + str);
				NUnit.Framework.Assert.AreEqual(text, claimedText);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWithJavaSerialization()
		{
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			LOG.info("Testing DefaultStringifier with Serializable Integer");
			//Integer implements Serializable
			int testInt = int.Parse(42);
			org.apache.hadoop.io.DefaultStringifier<int> stringifier = new org.apache.hadoop.io.DefaultStringifier
				<int>(conf, Sharpen.Runtime.getClassForType(typeof(int)));
			string str = stringifier.toString(testInt);
			int claimedInt = stringifier.fromString(str);
			LOG.info("String representation of the object: " + str);
			NUnit.Framework.Assert.AreEqual(testInt, claimedInt);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStoreLoad()
		{
			LOG.info("Testing DefaultStringifier#store() and #load()");
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization"
				);
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text("uninteresting test string"
				);
			string keyName = "test.defaultstringifier.key1";
			org.apache.hadoop.io.DefaultStringifier.store(conf, text, keyName);
			org.apache.hadoop.io.Text claimedText = org.apache.hadoop.io.DefaultStringifier.load
				<org.apache.hadoop.io.Text>(conf, keyName);
			NUnit.Framework.Assert.AreEqual("DefaultStringifier#load() or #store() might be flawed"
				, text, claimedText);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStoreLoadArray()
		{
			LOG.info("Testing DefaultStringifier#storeArray() and #loadArray()");
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			string keyName = "test.defaultstringifier.key2";
			int[] array = new int[] { 1, 2, 3, 4, 5 };
			org.apache.hadoop.io.DefaultStringifier.storeArray(conf, array, keyName);
			int[] claimedArray = org.apache.hadoop.io.DefaultStringifier.loadArray<int, int>(
				conf, keyName);
			for (int i = 0; i < array.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("two arrays are not equal", array[i], claimedArray
					[i]);
			}
		}
	}
}
