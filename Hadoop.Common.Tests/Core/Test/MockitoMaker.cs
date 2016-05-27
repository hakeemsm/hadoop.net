using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>
	/// Helper class to create one-liner stubs, so that instead of: <pre>
	/// SomeType someDescriptiveMock = mock(SomeType.class);
	/// when(someDescriptiveMock.someMethod()).thenReturn(someValue);</pre>
	/// <p>You can now do: <pre>
	/// SomeType someDescriptiveMock = make(stub(SomeType.class)
	/// .returning(someValue).from.someMethod());</pre>
	/// </summary>
	public class MockitoMaker
	{
		/// <summary>Create a mock object from a mocked method call.</summary>
		/// <?/>
		/// <param name="methodCall">for mocked object</param>
		/// <returns>mocked object</returns>
		public static T Make<T>(object methodCall)
		{
			MockitoMaker.StubBuilder<T> sb = MockitoMaker.StubBuilder.Current();
			Org.Mockito.Mockito.When(methodCall).ThenReturn(sb.firstReturn, sb.laterReturns);
			return (T)MockitoMaker.StubBuilder.Current().from;
		}

		/// <summary>Create a stub builder of a mocked object.</summary>
		/// <?/>
		/// <param name="target">class of the target object to be mocked</param>
		/// <returns>the stub builder of the mocked object</returns>
		public static MockitoMaker.StubBuilder<T> Stub<T>()
		{
			System.Type target = typeof(T);
			return new MockitoMaker.StubBuilder<T>(Org.Mockito.Mockito.Mock(target));
		}

		/// <summary>Builder class for stubs</summary>
		/// <?/>
		public class StubBuilder<T>
		{
			/// <summary>The target mock object</summary>
			public readonly T from;

			private sealed class _ThreadLocal_71 : ThreadLocal<MockitoMaker.StubBuilder>
			{
				public _ThreadLocal_71()
				{
				}

				// We want to be able to use this even when the tests are run in parallel.
				protected override MockitoMaker.StubBuilder InitialValue()
				{
					return new MockitoMaker.StubBuilder();
				}
			}

			private static readonly ThreadLocal<MockitoMaker.StubBuilder> tls = new _ThreadLocal_71
				();

			private object firstReturn = null;

			private object[] laterReturns = new object[] {  };

			/// <summary>Default constructor for the initial stub builder</summary>
			public StubBuilder()
			{
				this.from = null;
			}

			/// <summary>Construct a stub builder with a mock instance</summary>
			/// <param name="mockInstance">the mock object</param>
			public StubBuilder(T mockInstance)
			{
				tls.Set(this);
				this.from = mockInstance;
			}

			/// <summary>Get the current stub builder from thread local</summary>
			/// <?/>
			/// <returns>the stub builder of the mocked object</returns>
			public static MockitoMaker.StubBuilder<T> Current<T>()
			{
				return tls.Get();
			}

			/// <summary>Set the return value for the current stub builder</summary>
			/// <param name="value">the return value</param>
			/// <returns>the stub builder</returns>
			public virtual MockitoMaker.StubBuilder<T> Returning(object value)
			{
				this.firstReturn = value;
				return this;
			}

			/// <summary>Set the return values for the current stub builder</summary>
			/// <param name="value">the first return value</param>
			/// <param name="values">the return values for later invocations</param>
			/// <returns>the stub builder</returns>
			public virtual MockitoMaker.StubBuilder<T> Returning(object value, params object[]
				 values)
			{
				this.firstReturn = value;
				this.laterReturns = values;
				return this;
			}
		}
	}
}
