

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// The IdentityProvider creates identities for each schedulable
	/// by extracting fields and returning an identity string.
	/// </summary>
	/// <remarks>
	/// The IdentityProvider creates identities for each schedulable
	/// by extracting fields and returning an identity string.
	/// Implementers will be able to change how schedulers treat
	/// Schedulables.
	/// </remarks>
	public interface IdentityProvider
	{
		/// <summary>Return the string used for scheduling.</summary>
		/// <param name="obj">the schedulable to use.</param>
		/// <returns>string identity, or null if no identity could be made.</returns>
		string MakeIdentity(Schedulable obj);
	}
}
