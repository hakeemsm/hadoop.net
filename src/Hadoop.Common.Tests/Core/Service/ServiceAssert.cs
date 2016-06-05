using NUnit.Framework;


namespace Org.Apache.Hadoop.Service
{
	/// <summary>A set of assertions about the state of any service</summary>
	public class ServiceAssert : Assert
	{
		public static void AssertServiceStateCreated(Org.Apache.Hadoop.Service.Service service
			)
		{
			AssertServiceInState(service, Service.STATE.Notinited);
		}

		public static void AssertServiceStateInited(Org.Apache.Hadoop.Service.Service service
			)
		{
			AssertServiceInState(service, Service.STATE.Inited);
		}

		public static void AssertServiceStateStarted(Org.Apache.Hadoop.Service.Service service
			)
		{
			AssertServiceInState(service, Service.STATE.Started);
		}

		public static void AssertServiceStateStopped(Org.Apache.Hadoop.Service.Service service
			)
		{
			AssertServiceInState(service, Service.STATE.Stopped);
		}

		public static void AssertServiceInState(Org.Apache.Hadoop.Service.Service service
			, Service.STATE state)
		{
			NUnit.Framework.Assert.IsNotNull("Null service", service);
			Assert.Equal("Service in wrong state: " + service, state, service
				.GetServiceState());
		}

		/// <summary>
		/// Assert that the breakable service has entered a state exactly the number
		/// of time asserted.
		/// </summary>
		/// <param name="service">service -if null an assertion is raised.</param>
		/// <param name="state">state to check.</param>
		/// <param name="expected">expected count.</param>
		public static void AssertStateCount(BreakableService service, Service.STATE state
			, int expected)
		{
			NUnit.Framework.Assert.IsNotNull("Null service", service);
			int actual = service.GetCount(state);
			if (expected != actual)
			{
				NUnit.Framework.Assert.Fail("Expected entry count for state [" + state + "] of " 
					+ service + " to be " + expected + " but was " + actual);
			}
		}

		/// <summary>
		/// Assert that a service configuration contains a specific key; the value
		/// is ignored.
		/// </summary>
		/// <param name="service">service to check</param>
		/// <param name="key">key to look for</param>
		public static void AssertServiceConfigurationContains(Org.Apache.Hadoop.Service.Service
			 service, string key)
		{
			NUnit.Framework.Assert.IsNotNull("No option " + key + " in service configuration"
				, service.GetConfig().Get(key));
		}
	}
}
