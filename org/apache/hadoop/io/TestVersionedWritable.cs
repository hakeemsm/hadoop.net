using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for VersionedWritable.</summary>
	public class TestVersionedWritable : NUnit.Framework.TestCase
	{
		public TestVersionedWritable(string name)
			: base(name)
		{
		}

		/// <summary>Example class used in test cases below.</summary>
		public class SimpleVersionedWritable : org.apache.hadoop.io.VersionedWritable
		{
			private static readonly java.util.Random RANDOM = new java.util.Random();

			internal int state = RANDOM.nextInt();

			private static byte VERSION = 1;

			public override byte getVersion()
			{
				return VERSION;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				base.write(@out);
				// version.
				@out.writeInt(state);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				base.readFields(@in);
				// version
				this.state = @in.readInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public static org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable 
				read(java.io.DataInput @in)
			{
				org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable result = new org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
					();
				result.readFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable))
				{
					return false;
				}
				org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable other = (org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
					)o;
				return this.state == other.state;
			}
		}

		public class AdvancedVersionedWritable : org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
		{
			internal string shortTestString = "Now is the time for all good men to come to the aid of the Party";

			internal string longTestString = "Four score and twenty years ago. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah.";

			internal string compressableTestString = "Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. "
				 + "Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. " + 
				"Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. ";

			internal org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable containedObject
				 = new org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable();

			internal string[] testStringArray = new string[] { "The", "Quick", "Brown", "Fox"
				, "Jumped", "Over", "The", "Lazy", "Dog" };

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				base.write(@out);
				@out.writeUTF(shortTestString);
				org.apache.hadoop.io.WritableUtils.writeString(@out, longTestString);
				int comp = org.apache.hadoop.io.WritableUtils.writeCompressedString(@out, compressableTestString
					);
				System.Console.Out.WriteLine("Compression is " + comp + "%");
				containedObject.write(@out);
				// Warning if this is a recursive call, you need a null value.
				org.apache.hadoop.io.WritableUtils.writeStringArray(@out, testStringArray);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				base.readFields(@in);
				shortTestString = @in.readUTF();
				longTestString = org.apache.hadoop.io.WritableUtils.readString(@in);
				compressableTestString = org.apache.hadoop.io.WritableUtils.readCompressedString(
					@in);
				containedObject.readFields(@in);
				// Warning if this is a recursive call, you need a null value.
				testStringArray = org.apache.hadoop.io.WritableUtils.readStringArray(@in);
			}

			public override bool Equals(object o)
			{
				base.Equals(o);
				if (!shortTestString.Equals(((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
					)o).shortTestString))
				{
					return false;
				}
				if (!longTestString.Equals(((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
					)o).longTestString))
				{
					return false;
				}
				if (!compressableTestString.Equals(((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
					)o).compressableTestString))
				{
					return false;
				}
				if (testStringArray.Length != ((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
					)o).testStringArray.Length)
				{
					return false;
				}
				for (int i = 0; i < testStringArray.Length; i++)
				{
					if (!testStringArray[i].Equals(((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
						)o).testStringArray[i]))
					{
						return false;
					}
				}
				if (!containedObject.Equals(((org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
					)o).containedObject))
				{
					return false;
				}
				return true;
			}
		}

		public class SimpleVersionedWritableV2 : org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
		{
			internal static byte VERSION = 2;

			/* This one checks that version mismatch is thrown... */
			public override byte getVersion()
			{
				return VERSION;
			}
		}

		/// <summary>Test 1: Check that SimpleVersionedWritable.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSimpleVersionedWritable()
		{
			org.apache.hadoop.io.TestWritable.testWritable(new org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
				());
		}

		/// <summary>Test 2: Check that AdvancedVersionedWritable Works (well, why wouldn't it!).
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testAdvancedVersionedWritable()
		{
			org.apache.hadoop.io.TestWritable.testWritable(new org.apache.hadoop.io.TestVersionedWritable.AdvancedVersionedWritable
				());
		}

		/// <summary>Test 3: Check that SimpleVersionedWritable throws an Exception.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSimpleVersionedWritableMismatch()
		{
			org.apache.hadoop.io.TestVersionedWritable.testVersionedWritable(new org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritable
				(), new org.apache.hadoop.io.TestVersionedWritable.SimpleVersionedWritableV2());
		}

		/// <summary>Utility method for testing VersionedWritables.</summary>
		/// <exception cref="System.Exception"/>
		public static void testVersionedWritable(org.apache.hadoop.io.Writable before, org.apache.hadoop.io.Writable
			 after)
		{
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			before.write(dob);
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(dob.getData(), dob.getLength());
			try
			{
				after.readFields(dib);
			}
			catch (org.apache.hadoop.io.VersionMismatchException vmme)
			{
				System.Console.Out.WriteLine("Good, we expected this:" + vmme);
				return;
			}
			throw new System.Exception("A Version Mismatch Didn't Happen!");
		}
	}
}
