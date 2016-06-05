using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Util
{
	public class TestGenericsUtil : TestCase
	{
		public virtual void TestToArray()
		{
			//test a list of size 10
			IList<int> list = new AList<int>();
			for (int i = 0; i < 10; i++)
			{
				list.AddItem(i);
			}
			int[] arr = GenericsUtil.ToArray(list);
			for (int i_1 = 0; i_1 < arr.Length; i_1++)
			{
				Assert.Equal("Array has identical elements as input list", list
					[i_1], arr[i_1]);
			}
		}

		public virtual void TestWithEmptyList()
		{
			try
			{
				IList<string> list = new AList<string>();
				string[] arr = GenericsUtil.ToArray(list);
				Fail("Empty array should throw exception");
				System.Console.Out.WriteLine(arr);
			}
			catch (IndexOutOfRangeException)
			{
			}
		}

		//use arr so that compiler will not complain
		//test case is successful
		public virtual void TestWithEmptyList2()
		{
			IList<string> list = new AList<string>();
			//this method should not throw IndexOutOfBoundsException
			string[] arr = GenericsUtil.ToArray<string, string>(list);
			Assert.Equal("Assert list creation w/ no elements results in length 0"
				, 0, arr.Length);
		}

		/// <summary>This class uses generics</summary>
		private class GenericClass<T>
		{
			internal T dummy;

			internal IList<T> list = new AList<T>();

			internal virtual void Add(T item)
			{
				this.list.AddItem(item);
			}

			internal virtual T[] FuncThatUsesToArray()
			{
				T[] arr = GenericsUtil.ToArray(this.list);
				return arr;
			}

			internal GenericClass(TestGenericsUtil _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestGenericsUtil _enclosing;
		}

		public virtual void TestWithGenericClass()
		{
			TestGenericsUtil.GenericClass<string> testSubject = new TestGenericsUtil.GenericClass
				<string>(this);
			testSubject.Add("test1");
			testSubject.Add("test2");
			try
			{
				//this cast would fail, if we had not used GenericsUtil.toArray, since the
				//rmethod would return Object[] rather than String[]
				string[] arr = testSubject.FuncThatUsesToArray();
				Assert.Equal("test1", arr[0]);
				Assert.Equal("test2", arr[1]);
			}
			catch (InvalidCastException)
			{
				Fail("GenericsUtil#toArray() is not working for generic classes");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGenericOptionsParser()
		{
			GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), new string
				[] { "-jt" });
			Assert.Equal(0, parser.GetRemainingArgs().Length);
			//  test if -D accepts -Dx=y=z
			parser = new GenericOptionsParser(new Configuration(), new string[] { "-Dx=y=z" }
				);
			Assert.Equal("Options parser gets entire ='s expresion", "y=z"
				, parser.GetConfiguration().Get("x"));
		}

		public virtual void TestGetClass()
		{
			//test with Integer
			int x = 42;
			Type c = GenericsUtil.GetClass(x);
			Assert.Equal("Correct generic type is acquired from object", typeof(
				int), c);
			//test with GenericClass<Integer>
			TestGenericsUtil.GenericClass<int> testSubject = new TestGenericsUtil.GenericClass
				<int>(this);
			Type c2 = GenericsUtil.GetClass(testSubject);
			Assert.Equal("Inner generics are acquired from object.", typeof(
				TestGenericsUtil.GenericClass), c2);
		}
	}
}
