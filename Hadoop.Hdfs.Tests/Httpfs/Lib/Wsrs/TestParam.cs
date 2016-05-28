using System;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public class TestParam
	{
		/// <exception cref="System.Exception"/>
		private void Test<T>(Param<T> param, string name, string domain, T defaultValue, 
			T validValue, string invalidStrValue, string outOfRangeValue)
		{
			NUnit.Framework.Assert.AreEqual(name, param.GetName());
			NUnit.Framework.Assert.AreEqual(domain, param.GetDomain());
			NUnit.Framework.Assert.AreEqual(defaultValue, param.Value());
			NUnit.Framework.Assert.AreEqual(defaultValue, param.ParseParam(string.Empty));
			NUnit.Framework.Assert.AreEqual(defaultValue, param.ParseParam(null));
			NUnit.Framework.Assert.AreEqual(validValue, param.ParseParam(validValue.ToString(
				)));
			if (invalidStrValue != null)
			{
				try
				{
					param.ParseParam(invalidStrValue);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException)
				{
				}
				catch (Exception)
				{
					//NOP
					NUnit.Framework.Assert.Fail();
				}
			}
			if (outOfRangeValue != null)
			{
				try
				{
					param.ParseParam(outOfRangeValue);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException)
				{
				}
				catch (Exception)
				{
					//NOP
					NUnit.Framework.Assert.Fail();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBoolean()
		{
			Param<bool> param = new _BooleanParam_64("b", false);
			Test(param, "b", "a boolean", false, true, "x", null);
		}

		private sealed class _BooleanParam_64 : BooleanParam
		{
			public _BooleanParam_64(string baseArg1, bool baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByte()
		{
			Param<byte> param = new _ByteParam_71("B", unchecked((byte)1));
			Test(param, "B", "a byte", unchecked((byte)1), unchecked((byte)2), "x", "256");
		}

		private sealed class _ByteParam_71 : ByteParam
		{
			public _ByteParam_71(string baseArg1, byte baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShort()
		{
			Param<short> param = new _ShortParam_78("S", (short)1);
			Test(param, "S", "a short", (short)1, (short)2, "x", string.Empty + ((int)short.MaxValue
				 + 1));
			param = new _ShortParam_83("S", (short)1, 8);
			NUnit.Framework.Assert.AreEqual((short)0x3ff, param.Parse("01777"));
		}

		private sealed class _ShortParam_78 : ShortParam
		{
			public _ShortParam_78(string baseArg1, short baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		private sealed class _ShortParam_83 : ShortParam
		{
			public _ShortParam_83(string baseArg1, short baseArg2, int baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInteger()
		{
			Param<int> param = new _IntegerParam_91("I", 1);
			Test(param, "I", "an integer", 1, 2, "x", string.Empty + ((long)int.MaxValue + 1)
				);
		}

		private sealed class _IntegerParam_91 : IntegerParam
		{
			public _IntegerParam_91(string baseArg1, int baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLong()
		{
			Param<long> param = new _LongParam_98("L", 1L);
			Test(param, "L", "a long", 1L, 2L, "x", null);
		}

		private sealed class _LongParam_98 : LongParam
		{
			public _LongParam_98(string baseArg1, long baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		public enum ENUM
		{
			Foo,
			Bar
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnum()
		{
			EnumParam<TestParam.ENUM> param = new _EnumParam_109("e", typeof(TestParam.ENUM), 
				TestParam.ENUM.Foo);
			Test(param, "e", "FOO,BAR", TestParam.ENUM.Foo, TestParam.ENUM.Bar, "x", null);
		}

		private sealed class _EnumParam_109 : EnumParam<TestParam.ENUM>
		{
			public _EnumParam_109(string baseArg1, Type baseArg2, TestParam.ENUM baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestString()
		{
			Param<string> param = new _StringParam_116("s", "foo");
			Test(param, "s", "a string", "foo", "bar", null, null);
		}

		private sealed class _StringParam_116 : StringParam
		{
			public _StringParam_116(string baseArg1, string baseArg2)
				: base(baseArg1, baseArg2)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRegEx()
		{
			Param<string> param = new _StringParam_123("r", "aa", Sharpen.Pattern.Compile(".."
				));
			Test(param, "r", "..", "aa", "bb", "c", null);
		}

		private sealed class _StringParam_123 : StringParam
		{
			public _StringParam_123(string baseArg1, string baseArg2, Sharpen.Pattern baseArg3
				)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}
		}
	}
}
