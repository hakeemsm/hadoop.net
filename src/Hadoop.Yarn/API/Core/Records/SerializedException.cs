using System;
using System.Text;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	public abstract class SerializedException
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static SerializedException NewInstance(Exception e)
		{
			SerializedException exception = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<SerializedException
				>();
			exception.Init(e);
			return exception;
		}

		/// <summary>
		/// Constructs a new <code>SerializedException</code> with the specified detail
		/// message and cause.
		/// </summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void Init(string message, Exception cause);

		/// <summary>
		/// Constructs a new <code>SerializedException</code> with the specified detail
		/// message.
		/// </summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void Init(string message);

		/// <summary>Constructs a new <code>SerializedException</code> with the specified cause.
		/// 	</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void Init(Exception cause);

		/// <summary>Get the detail message string of this exception.</summary>
		/// <returns>the detail message string of this exception.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract string GetMessage();

		/// <summary>Get the backtrace of this exception.</summary>
		/// <returns>the backtrace of this exception.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract string GetRemoteTrace();

		/// <summary>
		/// Get the cause of this exception or null if the cause is nonexistent or
		/// unknown.
		/// </summary>
		/// <returns>the cause of this exception.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract SerializedException GetCause();

		/// <summary>Deserialize the exception to a new Throwable.</summary>
		/// <returns>the Throwable form of this serialized exception.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract Exception DeSerialize();

		private void Stringify(StringBuilder sb)
		{
			sb.Append(GetMessage()).Append("\n").Append(GetRemoteTrace());
			SerializedException cause = GetCause();
			if (cause != null)
			{
				sb.Append("Caused by: ");
				cause.Stringify(sb);
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(128);
			Stringify(sb);
			return sb.ToString();
		}
	}
}
