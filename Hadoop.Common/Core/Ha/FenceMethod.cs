using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// A fencing method is a method by which one node can forcibly prevent
	/// another node from making continued progress.
	/// </summary>
	/// <remarks>
	/// A fencing method is a method by which one node can forcibly prevent
	/// another node from making continued progress. This might be implemented
	/// by killing a process on the other node, by denying the other node's
	/// access to shared storage, or by accessing a PDU to cut the other node's
	/// power.
	/// <p>
	/// Since these methods are often vendor- or device-specific, operators
	/// may implement this interface in order to achieve fencing.
	/// <p>
	/// Fencing is configured by the operator as an ordered list of methods to
	/// attempt. Each method will be tried in turn, and the next in the list
	/// will only be attempted if the previous one fails. See
	/// <see cref="NodeFencer"/>
	/// for more information.
	/// <p>
	/// If an implementation also implements
	/// <see cref="Org.Apache.Hadoop.Conf.Configurable"/>
	/// then its
	/// <code>setConf</code> method will be called upon instantiation.
	/// </remarks>
	public interface FenceMethod
	{
		/// <summary>Verify that the given fencing method's arguments are valid.</summary>
		/// <param name="args">
		/// the arguments provided in the configuration. This may
		/// be null if the operator did not configure any arguments.
		/// </param>
		/// <exception cref="BadFencingConfigurationException">if the arguments are invalid</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		void CheckArgs(string args);

		/// <summary>Attempt to fence the target node.</summary>
		/// <param name="serviceAddr">the address (host:ipcport) of the service to fence</param>
		/// <param name="args">
		/// the configured arguments, which were checked at startup by
		/// <see cref="CheckArgs(string)"/>
		/// </param>
		/// <returns>
		/// true if fencing was successful, false if unsuccessful or
		/// indeterminate
		/// </returns>
		/// <exception cref="BadFencingConfigurationException">
		/// if the configuration was
		/// determined to be invalid only at runtime
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		bool TryFence(HAServiceTarget target, string args);
	}
}
