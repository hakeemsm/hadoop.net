using System;
using System.IO;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for VersionedWritable.</summary>
	public class TestVersionedWritable : TestCase
	{
		public TestVersionedWritable(string name)
			: base(name)
		{
		}

		/// <summary>Example class used in test cases below.</summary>
		public class SimpleVersionedWritable : VersionedWritable
		{
			private static readonly Random Random = new Random();

			internal int state = Random.Next();

			private static byte Version = 1;

			public override byte GetVersion()
			{
				return Version;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(BinaryWriter @out)
			{
				base.Write(@out);
				// version.
				@out.WriteInt(state);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
				// version
				this.state = @in.ReadInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public static TestVersionedWritable.SimpleVersionedWritable Read(BinaryReader @in)
			{
				TestVersionedWritable.SimpleVersionedWritable result = new TestVersionedWritable.SimpleVersionedWritable
					();
				result.ReadFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is TestVersionedWritable.SimpleVersionedWritable))
				{
					return false;
				}
				TestVersionedWritable.SimpleVersionedWritable other = (TestVersionedWritable.SimpleVersionedWritable
					)o;
				return this.state == other.state;
			}
		}

		public class AdvancedVersionedWritable : TestVersionedWritable.SimpleVersionedWritable
		{
			internal string shortTestString = "Now is the time for all good men to come to the aid of the Party";

			internal string longTestString = "Four score and twenty years ago. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah.";

			internal string compressableTestString = "Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. "
				 + "Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. " + 
				"Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. Blah. ";

			internal TestVersionedWritable.SimpleVersionedWritable containedObject = new TestVersionedWritable.SimpleVersionedWritable
				();

			internal string[] testStringArray = new string[] { "The", "Quick", "Brown", "Fox"
				, "Jumped", "Over", "The", "Lazy", "Dog" };

			/// <exception cref="System.IO.IOException"/>
			public override void Write(BinaryWriter @out)
			{
				base.Write(@out);
				@out.WriteUTF(shortTestString);
				WritableUtils.WriteString(@out, longTestString);
				int comp = WritableUtils.WriteCompressedString(@out, compressableTestString);
				System.Console.Out.WriteLine("Compression is " + comp + "%");
				containedObject.Write(@out);
				// Warning if this is a recursive call, you need a null value.
				WritableUtils.WriteStringArray(@out, testStringArray);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
				shortTestString = @in.ReadUTF();
				longTestString = WritableUtils.ReadString(@in);
				compressableTestString = WritableUtils.ReadCompressedString(@in);
				containedObject.ReadFields(@in);
				// Warning if this is a recursive call, you need a null value.
				testStringArray = WritableUtils.ReadStringArray(@in);
			}

			public override bool Equals(object o)
			{
				base.Equals(o);
				if (!shortTestString.Equals(((TestVersionedWritable.AdvancedVersionedWritable)o).
					shortTestString))
				{
					return false;
				}
				if (!longTestString.Equals(((TestVersionedWritable.AdvancedVersionedWritable)o).longTestString
					))
				{
					return false;
				}
				if (!compressableTestString.Equals(((TestVersionedWritable.AdvancedVersionedWritable
					)o).compressableTestString))
				{
					return false;
				}
				if (testStringArray.Length != ((TestVersionedWritable.AdvancedVersionedWritable)o
					).testStringArray.Length)
				{
					return false;
				}
				for (int i = 0; i < testStringArray.Length; i++)
				{
					if (!testStringArray[i].Equals(((TestVersionedWritable.AdvancedVersionedWritable)
						o).testStringArray[i]))
					{
						return false;
					}
				}
				if (!containedObject.Equals(((TestVersionedWritable.AdvancedVersionedWritable)o).
					containedObject))
				{
					return false;
				}
				return true;
			}
		}

		public class SimpleVersionedWritableV2 : TestVersionedWritable.SimpleVersionedWritable
		{
			internal static byte Version = 2;

			/* This one checks that version mismatch is thrown... */
			public override byte GetVersion()
			{
				return Version;
			}
		}

		/// <summary>Test 1: Check that SimpleVersionedWritable.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleVersionedWritable()
		{
			TestWritable.TestWritable(new TestVersionedWritable.SimpleVersionedWritable());
		}

		/// <summary>Test 2: Check that AdvancedVersionedWritable Works (well, why wouldn't it!).
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAdvancedVersionedWritable()
		{
			TestWritable.TestWritable(new TestVersionedWritable.AdvancedVersionedWritable());
		}

		/// <summary>Test 3: Check that SimpleVersionedWritable throws an Exception.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleVersionedWritableMismatch()
		{
			TestVersionedWritable.TestVersionedWritable(new TestVersionedWritable.SimpleVersionedWritable
				(), new TestVersionedWritable.SimpleVersionedWritableV2());
		}

		/// <summary>Utility method for testing VersionedWritables.</summary>
		/// <exception cref="System.Exception"/>
		public static void TestVersionedWritable(Writable before, Writable after)
		{
			DataOutputBuffer dob = new DataOutputBuffer();
			before.Write(dob);
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), dob.GetLength());
			try
			{
				after.ReadFields(dib);
			}
			catch (VersionMismatchException vmme)
			{
				System.Console.Out.WriteLine("Good, we expected this:" + vmme);
				return;
			}
			throw new Exception("A Version Mismatch Didn't Happen!");
		}
	}
}
