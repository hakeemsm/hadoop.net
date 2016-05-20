using Sharpen;

namespace org.apache.hadoop.io.retry
{
	public abstract class UnreliableInterface
	{
		[System.Serializable]
		public class UnreliableException : System.Exception
		{
			private const long serialVersionUID = 1L;

			private string identifier;

			public UnreliableException()
			{
			}

			public UnreliableException(string identifier)
			{
				// no body
				this.identifier = identifier;
			}

			public override string Message
			{
				get
				{
					return identifier;
				}
			}
		}

		[System.Serializable]
		public class FatalException : org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		{
			private const long serialVersionUID = 1L;
			// no body
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void alwaysSucceeds();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.FatalException"/>
		public abstract void alwaysFailsWithFatalException();

		/// <exception cref="org.apache.hadoop.ipc.RemoteException"/>
		public abstract void alwaysFailsWithRemoteFatalException();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void failsOnceThenSucceeds();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract bool failsOnceThenSucceedsWithReturnValue();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void failsTenTimesThenSucceeds();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract string succeedsOnceThenFailsReturningString();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract string succeedsOnceThenFailsReturningStringIdempotent();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract string succeedsTenTimesThenFailsReturningString();

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract string failsIfIdentifierDoesntMatch(string identifier);

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void nonIdempotentVoidFailsIfIdentifierDoesntMatch(string identifier
			);
	}

	public static class UnreliableInterfaceConstants
	{
	}
}
