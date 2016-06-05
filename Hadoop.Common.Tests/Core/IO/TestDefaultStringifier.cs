using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestDefaultStringifier : TestCase
	{
		private static Configuration conf = new Configuration();

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDefaultStringifier
			));

		private char[] alphabet = "abcdefghijklmnopqrstuvwxyz".ToCharArray();

		/// <exception cref="System.Exception"/>
		public virtual void TestWithWritable()
		{
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization"
				);
			Log.Info("Testing DefaultStringifier with Text");
			Random random = new Random();
			//test with a Text
			for (int i = 0; i < 10; i++)
			{
				//generate a random string
				StringBuilder builder = new StringBuilder();
				int strLen = random.Next(40);
				for (int j = 0; j < strLen; j++)
				{
					builder.Append(alphabet[random.Next(alphabet.Length)]);
				}
				Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text(builder.ToString()
					);
				DefaultStringifier<Org.Apache.Hadoop.IO.Text> stringifier = new DefaultStringifier
					<Org.Apache.Hadoop.IO.Text>(conf, typeof(Org.Apache.Hadoop.IO.Text));
				string str = stringifier.ToString(text);
				Org.Apache.Hadoop.IO.Text claimedText = stringifier.FromString(str);
				Log.Info("Object: " + text);
				Log.Info("String representation of the object: " + str);
				Assert.Equal(text, claimedText);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithJavaSerialization()
		{
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			Log.Info("Testing DefaultStringifier with Serializable Integer");
			//Integer implements Serializable
			int testInt = Sharpen.Extensions.ValueOf(42);
			DefaultStringifier<int> stringifier = new DefaultStringifier<int>(conf, typeof(int
				));
			string str = stringifier.ToString(testInt);
			int claimedInt = stringifier.FromString(str);
			Log.Info("String representation of the object: " + str);
			Assert.Equal(testInt, claimedInt);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStoreLoad()
		{
			Log.Info("Testing DefaultStringifier#store() and #load()");
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization"
				);
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text("uninteresting test string"
				);
			string keyName = "test.defaultstringifier.key1";
			DefaultStringifier.Store(conf, text, keyName);
			Org.Apache.Hadoop.IO.Text claimedText = DefaultStringifier.Load<Org.Apache.Hadoop.IO.Text
				>(conf, keyName);
			Assert.Equal("DefaultStringifier#load() or #store() might be flawed"
				, text, claimedText);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStoreLoadArray()
		{
			Log.Info("Testing DefaultStringifier#storeArray() and #loadArray()");
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			string keyName = "test.defaultstringifier.key2";
			int[] array = new int[] { 1, 2, 3, 4, 5 };
			DefaultStringifier.StoreArray(conf, array, keyName);
			int[] claimedArray = DefaultStringifier.LoadArray<int, int>(conf, keyName);
			for (int i = 0; i < array.Length; i++)
			{
				Assert.Equal("two arrays are not equal", array[i], claimedArray
					[i]);
			}
		}
	}
}
