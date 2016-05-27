using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Group;
using Org.Jboss.Netty.Handler.Timeout;
using Sharpen;

namespace Org.Apache.Hadoop.Portmap
{
	internal sealed class RpcProgramPortmap : IdleStateAwareChannelUpstreamHandler
	{
		internal const int Program = 100000;

		internal const int Version = 2;

		internal const int PmapprocNull = 0;

		internal const int PmapprocSet = 1;

		internal const int PmapprocUnset = 2;

		internal const int PmapprocGetport = 3;

		internal const int PmapprocDump = 4;

		internal const int PmapprocGetversaddr = 9;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Portmap.RpcProgramPortmap
			));

		private readonly ConcurrentHashMap<string, PortmapMapping> map = new ConcurrentHashMap
			<string, PortmapMapping>();

		/// <summary>ChannelGroup that remembers all active channels for gracefully shutdown.
		/// 	</summary>
		private readonly ChannelGroup allChannels;

		internal RpcProgramPortmap(ChannelGroup allChannels)
		{
			this.allChannels = allChannels;
			PortmapMapping m = new PortmapMapping(Program, Version, PortmapMapping.TransportTcp
				, RpcProgram.RpcbPort);
			PortmapMapping m1 = new PortmapMapping(Program, Version, PortmapMapping.TransportUdp
				, RpcProgram.RpcbPort);
			map[PortmapMapping.Key(m)] = m;
			map[PortmapMapping.Key(m1)] = m1;
		}

		/// <summary>This procedure does no work.</summary>
		/// <remarks>
		/// This procedure does no work. By convention, procedure zero of any protocol
		/// takes no parameters and returns no results.
		/// </remarks>
		private XDR NullOp(int xid, XDR @in, XDR @out)
		{
			return PortmapResponse.VoidReply(@out, xid);
		}

		/// <summary>
		/// When a program first becomes available on a machine, it registers itself
		/// with the port mapper program on the same machine.
		/// </summary>
		/// <remarks>
		/// When a program first becomes available on a machine, it registers itself
		/// with the port mapper program on the same machine. The program passes its
		/// program number "prog", version number "vers", transport protocol number
		/// "prot", and the port "port" on which it awaits service request. The
		/// procedure returns a boolean reply whose value is "TRUE" if the procedure
		/// successfully established the mapping and "FALSE" otherwise. The procedure
		/// refuses to establish a mapping if one already exists for the tuple
		/// "(prog, vers, prot)".
		/// </remarks>
		private XDR Set(int xid, XDR @in, XDR @out)
		{
			PortmapMapping mapping = PortmapRequest.Mapping(@in);
			string key = PortmapMapping.Key(mapping);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Portmap set key=" + key);
			}
			map[key] = mapping;
			return PortmapResponse.IntReply(@out, xid, mapping.GetPort());
		}

		/// <summary>
		/// When a program becomes unavailable, it should unregister itself with the
		/// port mapper program on the same machine.
		/// </summary>
		/// <remarks>
		/// When a program becomes unavailable, it should unregister itself with the
		/// port mapper program on the same machine. The parameters and results have
		/// meanings identical to those of "PMAPPROC_SET". The protocol and port number
		/// fields of the argument are ignored.
		/// </remarks>
		private XDR Unset(int xid, XDR @in, XDR @out)
		{
			PortmapMapping mapping = PortmapRequest.Mapping(@in);
			string key = PortmapMapping.Key(mapping);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Portmap remove key=" + key);
			}
			Sharpen.Collections.Remove(map, key);
			return PortmapResponse.BooleanReply(@out, xid, true);
		}

		/// <summary>
		/// Given a program number "prog", version number "vers", and transport
		/// protocol number "prot", this procedure returns the port number on which the
		/// program is awaiting call requests.
		/// </summary>
		/// <remarks>
		/// Given a program number "prog", version number "vers", and transport
		/// protocol number "prot", this procedure returns the port number on which the
		/// program is awaiting call requests. A port value of zeros means the program
		/// has not been registered. The "port" field of the argument is ignored.
		/// </remarks>
		private XDR Getport(int xid, XDR @in, XDR @out)
		{
			PortmapMapping mapping = PortmapRequest.Mapping(@in);
			string key = PortmapMapping.Key(mapping);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Portmap GETPORT key=" + key + " " + mapping);
			}
			PortmapMapping value = map[key];
			int res = 0;
			if (value != null)
			{
				res = value.GetPort();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Found mapping for key: " + key + " port:" + res);
				}
			}
			else
			{
				Log.Warn("Warning, no mapping for key: " + key);
			}
			return PortmapResponse.IntReply(@out, xid, res);
		}

		/// <summary>This procedure enumerates all entries in the port mapper's database.</summary>
		/// <remarks>
		/// This procedure enumerates all entries in the port mapper's database. The
		/// procedure takes no parameters and returns a list of program, version,
		/// protocol, and port values.
		/// </remarks>
		private XDR Dump(int xid, XDR @in, XDR @out)
		{
			PortmapMapping[] pmapList = Sharpen.Collections.ToArray(map.Values, new PortmapMapping
				[0]);
			return PortmapResponse.PmapList(@out, xid, pmapList);
		}

		/// <exception cref="System.Exception"/>
		public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
		{
			RpcInfo info = (RpcInfo)e.GetMessage();
			RpcCall rpcCall = (RpcCall)info.Header();
			int portmapProc = rpcCall.GetProcedure();
			int xid = rpcCall.GetXid();
			XDR @in = new XDR(info.Data().ToByteBuffer().AsReadOnlyBuffer(), XDR.State.Reading
				);
			XDR @out = new XDR();
			if (portmapProc == PmapprocNull)
			{
				@out = NullOp(xid, @in, @out);
			}
			else
			{
				if (portmapProc == PmapprocSet)
				{
					@out = Set(xid, @in, @out);
				}
				else
				{
					if (portmapProc == PmapprocUnset)
					{
						@out = Unset(xid, @in, @out);
					}
					else
					{
						if (portmapProc == PmapprocDump)
						{
							@out = Dump(xid, @in, @out);
						}
						else
						{
							if (portmapProc == PmapprocGetport)
							{
								@out = Getport(xid, @in, @out);
							}
							else
							{
								if (portmapProc == PmapprocGetversaddr)
								{
									@out = Getport(xid, @in, @out);
								}
								else
								{
									Log.Info("PortmapHandler unknown rpc procedure=" + portmapProc);
									RpcAcceptedReply reply = RpcAcceptedReply.GetInstance(xid, RpcAcceptedReply.AcceptState
										.ProcUnavail, new VerifierNone());
									reply.Write(@out);
								}
							}
						}
					}
				}
			}
			ChannelBuffer buf = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer());
			RpcResponse rsp = new RpcResponse(buf, info.RemoteAddress());
			RpcUtil.SendRpcResponse(ctx, rsp);
		}

		/// <exception cref="System.Exception"/>
		public override void ChannelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
		{
			allChannels.AddItem(e.GetChannel());
		}

		/// <exception cref="System.Exception"/>
		public override void ChannelIdle(ChannelHandlerContext ctx, IdleStateEvent e)
		{
			if (e.GetState() == IdleState.AllIdle)
			{
				e.GetChannel().Close();
			}
		}

		public override void ExceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
		{
			Log.Warn("Encountered ", e.GetCause());
			e.GetChannel().Close();
		}
	}
}
