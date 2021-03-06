using System;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS.Permission
{
	public class TestFsPermission : TestCase
	{
		public virtual void TestFsAction()
		{
			//implies
			foreach (FsAction a in FsAction.Values())
			{
				Assert.True(FsAction.All.Implies(a));
			}
			foreach (FsAction a_1 in FsAction.Values())
			{
				Assert.True(a_1 == FsAction.None ? FsAction.None.Implies(a_1) : 
					!FsAction.None.Implies(a_1));
			}
			foreach (FsAction a_2 in FsAction.Values())
			{
				Assert.True(a_2 == FsAction.ReadExecute || a_2 == FsAction.Read
					 || a_2 == FsAction.Execute || a_2 == FsAction.None ? FsAction.ReadExecute.Implies
					(a_2) : !FsAction.ReadExecute.Implies(a_2));
			}
			//masks
			Assert.Equal(FsAction.Execute, FsAction.Execute.And(FsAction.ReadExecute
				));
			Assert.Equal(FsAction.Read, FsAction.Read.And(FsAction.ReadExecute
				));
			Assert.Equal(FsAction.None, FsAction.Write.And(FsAction.ReadExecute
				));
			Assert.Equal(FsAction.Read, FsAction.ReadExecute.And(FsAction.
				ReadWrite));
			Assert.Equal(FsAction.None, FsAction.ReadExecute.And(FsAction.
				Write));
			Assert.Equal(FsAction.WriteExecute, FsAction.All.And(FsAction.
				WriteExecute));
		}

		/// <summary>
		/// Ensure that when manually specifying permission modes we get
		/// the expected values back out for all combinations
		/// </summary>
		public virtual void TestConvertingPermissions()
		{
			for (short s = 0; s <= 0x3ff; s++)
			{
				Assert.Equal(s, new FsPermission(s).ToShort());
			}
			short s_1 = 0;
			foreach (bool sb in new bool[] { false, true })
			{
				foreach (FsAction u in FsAction.Values())
				{
					foreach (FsAction g in FsAction.Values())
					{
						foreach (FsAction o in FsAction.Values())
						{
							// Cover constructor with sticky bit.
							FsPermission f = new FsPermission(u, g, o, sb);
							Assert.Equal(s_1, f.ToShort());
							FsPermission f2 = new FsPermission(f);
							Assert.Equal(s_1, f2.ToShort());
							s_1++;
						}
					}
				}
			}
		}

		public virtual void TestSpecialBitsToString()
		{
			foreach (bool sb in new bool[] { false, true })
			{
				foreach (FsAction u in FsAction.Values())
				{
					foreach (FsAction g in FsAction.Values())
					{
						foreach (FsAction o in FsAction.Values())
						{
							FsPermission f = new FsPermission(u, g, o, sb);
							string fString = f.ToString();
							// Check that sticky bit is represented correctly.
							if (f.GetStickyBit() && f.GetOtherAction().Implies(FsAction.Execute))
							{
								Assert.Equal('t', fString[8]);
							}
							else
							{
								if (f.GetStickyBit() && !f.GetOtherAction().Implies(FsAction.Execute))
								{
									Assert.Equal('T', fString[8]);
								}
								else
								{
									if (!f.GetStickyBit() && f.GetOtherAction().Implies(FsAction.Execute))
									{
										Assert.Equal('x', fString[8]);
									}
									else
									{
										Assert.Equal('-', fString[8]);
									}
								}
							}
							Assert.Equal(9, fString.Length);
						}
					}
				}
			}
		}

		public virtual void TestFsPermission()
		{
			string symbolic = "-rwxrwxrwx";
			for (int i = 0; i < (1 << 10); i++)
			{
				StringBuilder b = new StringBuilder("----------");
				string binary = string.Format("%11s", int.ToBinaryString(i));
				string permBinary = Runtime.Substring(binary, 2, binary.Length);
				int len = permBinary.Length;
				for (int j = 0; j < len; j++)
				{
					if (permBinary[j] == '1')
					{
						int k = 9 - (len - 1 - j);
						Runtime.SetCharAt(b, k, symbolic[k]);
					}
				}
				// Check for sticky bit.
				if (binary[1] == '1')
				{
					char replacement = b[9] == 'x' ? 't' : 'T';
					Runtime.SetCharAt(b, 9, replacement);
				}
				Assert.Equal(i, FsPermission.ValueOf(b.ToString()).ToShort());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUMaskParser()
		{
			Configuration conf = new Configuration();
			// Ensure that we get the right octal values back for all legal values
			foreach (FsAction u in FsAction.Values())
			{
				foreach (FsAction g in FsAction.Values())
				{
					foreach (FsAction o in FsAction.Values())
					{
						FsPermission f = new FsPermission(u, g, o);
						string asOctal = string.Format("%1$03o", f.ToShort());
						conf.Set(FsPermission.UmaskLabel, asOctal);
						FsPermission fromConf = FsPermission.GetUMask(conf);
						Assert.Equal(f, fromConf);
					}
				}
			}
		}

		public virtual void TestSymbolicUmasks()
		{
			Configuration conf = new Configuration();
			// Test some symbolic to octal settings
			// Symbolic umask list is generated in linux shell using by the command:
			// umask 0; umask <octal number>; umask -S
			string[][] symbolic = new string[][] { new string[] { "a+rw", "111" }, new string
				[] { "u=rwx,g=rwx,o=rwx", "0" }, new string[] { "u=rwx,g=rwx,o=rw", "1" }, new string
				[] { "u=rwx,g=rwx,o=rx", "2" }, new string[] { "u=rwx,g=rwx,o=r", "3" }, new string
				[] { "u=rwx,g=rwx,o=wx", "4" }, new string[] { "u=rwx,g=rwx,o=w", "5" }, new string
				[] { "u=rwx,g=rwx,o=x", "6" }, new string[] { "u=rwx,g=rwx,o=", "7" }, new string
				[] { "u=rwx,g=rw,o=rwx", "10" }, new string[] { "u=rwx,g=rw,o=rw", "11" }, new string
				[] { "u=rwx,g=rw,o=rx", "12" }, new string[] { "u=rwx,g=rw,o=r", "13" }, new string
				[] { "u=rwx,g=rw,o=wx", "14" }, new string[] { "u=rwx,g=rw,o=w", "15" }, new string
				[] { "u=rwx,g=rw,o=x", "16" }, new string[] { "u=rwx,g=rw,o=", "17" }, new string
				[] { "u=rwx,g=rx,o=rwx", "20" }, new string[] { "u=rwx,g=rx,o=rw", "21" }, new string
				[] { "u=rwx,g=rx,o=rx", "22" }, new string[] { "u=rwx,g=rx,o=r", "23" }, new string
				[] { "u=rwx,g=rx,o=wx", "24" }, new string[] { "u=rwx,g=rx,o=w", "25" }, new string
				[] { "u=rwx,g=rx,o=x", "26" }, new string[] { "u=rwx,g=rx,o=", "27" }, new string
				[] { "u=rwx,g=r,o=rwx", "30" }, new string[] { "u=rwx,g=r,o=rw", "31" }, new string
				[] { "u=rwx,g=r,o=rx", "32" }, new string[] { "u=rwx,g=r,o=r", "33" }, new string
				[] { "u=rwx,g=r,o=wx", "34" }, new string[] { "u=rwx,g=r,o=w", "35" }, new string
				[] { "u=rwx,g=r,o=x", "36" }, new string[] { "u=rwx,g=r,o=", "37" }, new string[
				] { "u=rwx,g=wx,o=rwx", "40" }, new string[] { "u=rwx,g=wx,o=rw", "41" }, new string
				[] { "u=rwx,g=wx,o=rx", "42" }, new string[] { "u=rwx,g=wx,o=r", "43" }, new string
				[] { "u=rwx,g=wx,o=wx", "44" }, new string[] { "u=rwx,g=wx,o=w", "45" }, new string
				[] { "u=rwx,g=wx,o=x", "46" }, new string[] { "u=rwx,g=wx,o=", "47" }, new string
				[] { "u=rwx,g=w,o=rwx", "50" }, new string[] { "u=rwx,g=w,o=rw", "51" }, new string
				[] { "u=rwx,g=w,o=rx", "52" }, new string[] { "u=rwx,g=w,o=r", "53" }, new string
				[] { "u=rwx,g=w,o=wx", "54" }, new string[] { "u=rwx,g=w,o=w", "55" }, new string
				[] { "u=rwx,g=w,o=x", "56" }, new string[] { "u=rwx,g=w,o=", "57" }, new string[
				] { "u=rwx,g=x,o=rwx", "60" }, new string[] { "u=rwx,g=x,o=rw", "61" }, new string
				[] { "u=rwx,g=x,o=rx", "62" }, new string[] { "u=rwx,g=x,o=r", "63" }, new string
				[] { "u=rwx,g=x,o=wx", "64" }, new string[] { "u=rwx,g=x,o=w", "65" }, new string
				[] { "u=rwx,g=x,o=x", "66" }, new string[] { "u=rwx,g=x,o=", "67" }, new string[
				] { "u=rwx,g=,o=rwx", "70" }, new string[] { "u=rwx,g=,o=rw", "71" }, new string
				[] { "u=rwx,g=,o=rx", "72" }, new string[] { "u=rwx,g=,o=r", "73" }, new string[
				] { "u=rwx,g=,o=wx", "74" }, new string[] { "u=rwx,g=,o=w", "75" }, new string[]
				 { "u=rwx,g=,o=x", "76" }, new string[] { "u=rwx,g=,o=", "77" }, new string[] { 
				"u=rw,g=rwx,o=rwx", "100" }, new string[] { "u=rw,g=rwx,o=rw", "101" }, new string
				[] { "u=rw,g=rwx,o=rx", "102" }, new string[] { "u=rw,g=rwx,o=r", "103" }, new string
				[] { "u=rw,g=rwx,o=wx", "104" }, new string[] { "u=rw,g=rwx,o=w", "105" }, new string
				[] { "u=rw,g=rwx,o=x", "106" }, new string[] { "u=rw,g=rwx,o=", "107" }, new string
				[] { "u=rw,g=rw,o=rwx", "110" }, new string[] { "u=rw,g=rw,o=rw", "111" }, new string
				[] { "u=rw,g=rw,o=rx", "112" }, new string[] { "u=rw,g=rw,o=r", "113" }, new string
				[] { "u=rw,g=rw,o=wx", "114" }, new string[] { "u=rw,g=rw,o=w", "115" }, new string
				[] { "u=rw,g=rw,o=x", "116" }, new string[] { "u=rw,g=rw,o=", "117" }, new string
				[] { "u=rw,g=rx,o=rwx", "120" }, new string[] { "u=rw,g=rx,o=rw", "121" }, new string
				[] { "u=rw,g=rx,o=rx", "122" }, new string[] { "u=rw,g=rx,o=r", "123" }, new string
				[] { "u=rw,g=rx,o=wx", "124" }, new string[] { "u=rw,g=rx,o=w", "125" }, new string
				[] { "u=rw,g=rx,o=x", "126" }, new string[] { "u=rw,g=rx,o=", "127" }, new string
				[] { "u=rw,g=r,o=rwx", "130" }, new string[] { "u=rw,g=r,o=rw", "131" }, new string
				[] { "u=rw,g=r,o=rx", "132" }, new string[] { "u=rw,g=r,o=r", "133" }, new string
				[] { "u=rw,g=r,o=wx", "134" }, new string[] { "u=rw,g=r,o=w", "135" }, new string
				[] { "u=rw,g=r,o=x", "136" }, new string[] { "u=rw,g=r,o=", "137" }, new string[
				] { "u=rw,g=wx,o=rwx", "140" }, new string[] { "u=rw,g=wx,o=rw", "141" }, new string
				[] { "u=rw,g=wx,o=rx", "142" }, new string[] { "u=rw,g=wx,o=r", "143" }, new string
				[] { "u=rw,g=wx,o=wx", "144" }, new string[] { "u=rw,g=wx,o=w", "145" }, new string
				[] { "u=rw,g=wx,o=x", "146" }, new string[] { "u=rw,g=wx,o=", "147" }, new string
				[] { "u=rw,g=w,o=rwx", "150" }, new string[] { "u=rw,g=w,o=rw", "151" }, new string
				[] { "u=rw,g=w,o=rx", "152" }, new string[] { "u=rw,g=w,o=r", "153" }, new string
				[] { "u=rw,g=w,o=wx", "154" }, new string[] { "u=rw,g=w,o=w", "155" }, new string
				[] { "u=rw,g=w,o=x", "156" }, new string[] { "u=rw,g=w,o=", "157" }, new string[
				] { "u=rw,g=x,o=rwx", "160" }, new string[] { "u=rw,g=x,o=rw", "161" }, new string
				[] { "u=rw,g=x,o=rx", "162" }, new string[] { "u=rw,g=x,o=r", "163" }, new string
				[] { "u=rw,g=x,o=wx", "164" }, new string[] { "u=rw,g=x,o=w", "165" }, new string
				[] { "u=rw,g=x,o=x", "166" }, new string[] { "u=rw,g=x,o=", "167" }, new string[
				] { "u=rw,g=,o=rwx", "170" }, new string[] { "u=rw,g=,o=rw", "171" }, new string
				[] { "u=rw,g=,o=rx", "172" }, new string[] { "u=rw,g=,o=r", "173" }, new string[
				] { "u=rw,g=,o=wx", "174" }, new string[] { "u=rw,g=,o=w", "175" }, new string[]
				 { "u=rw,g=,o=x", "176" }, new string[] { "u=rw,g=,o=", "177" }, new string[] { 
				"u=rx,g=rwx,o=rwx", "200" }, new string[] { "u=rx,g=rwx,o=rw", "201" }, new string
				[] { "u=rx,g=rwx,o=rx", "202" }, new string[] { "u=rx,g=rwx,o=r", "203" }, new string
				[] { "u=rx,g=rwx,o=wx", "204" }, new string[] { "u=rx,g=rwx,o=w", "205" }, new string
				[] { "u=rx,g=rwx,o=x", "206" }, new string[] { "u=rx,g=rwx,o=", "207" }, new string
				[] { "u=rx,g=rw,o=rwx", "210" }, new string[] { "u=rx,g=rw,o=rw", "211" }, new string
				[] { "u=rx,g=rw,o=rx", "212" }, new string[] { "u=rx,g=rw,o=r", "213" }, new string
				[] { "u=rx,g=rw,o=wx", "214" }, new string[] { "u=rx,g=rw,o=w", "215" }, new string
				[] { "u=rx,g=rw,o=x", "216" }, new string[] { "u=rx,g=rw,o=", "217" }, new string
				[] { "u=rx,g=rx,o=rwx", "220" }, new string[] { "u=rx,g=rx,o=rw", "221" }, new string
				[] { "u=rx,g=rx,o=rx", "222" }, new string[] { "u=rx,g=rx,o=r", "223" }, new string
				[] { "u=rx,g=rx,o=wx", "224" }, new string[] { "u=rx,g=rx,o=w", "225" }, new string
				[] { "u=rx,g=rx,o=x", "226" }, new string[] { "u=rx,g=rx,o=", "227" }, new string
				[] { "u=rx,g=r,o=rwx", "230" }, new string[] { "u=rx,g=r,o=rw", "231" }, new string
				[] { "u=rx,g=r,o=rx", "232" }, new string[] { "u=rx,g=r,o=r", "233" }, new string
				[] { "u=rx,g=r,o=wx", "234" }, new string[] { "u=rx,g=r,o=w", "235" }, new string
				[] { "u=rx,g=r,o=x", "236" }, new string[] { "u=rx,g=r,o=", "237" }, new string[
				] { "u=rx,g=wx,o=rwx", "240" }, new string[] { "u=rx,g=wx,o=rw", "241" }, new string
				[] { "u=rx,g=wx,o=rx", "242" }, new string[] { "u=rx,g=wx,o=r", "243" }, new string
				[] { "u=rx,g=wx,o=wx", "244" }, new string[] { "u=rx,g=wx,o=w", "245" }, new string
				[] { "u=rx,g=wx,o=x", "246" }, new string[] { "u=rx,g=wx,o=", "247" }, new string
				[] { "u=rx,g=w,o=rwx", "250" }, new string[] { "u=rx,g=w,o=rw", "251" }, new string
				[] { "u=rx,g=w,o=rx", "252" }, new string[] { "u=rx,g=w,o=r", "253" }, new string
				[] { "u=rx,g=w,o=wx", "254" }, new string[] { "u=rx,g=w,o=w", "255" }, new string
				[] { "u=rx,g=w,o=x", "256" }, new string[] { "u=rx,g=w,o=", "257" }, new string[
				] { "u=rx,g=x,o=rwx", "260" }, new string[] { "u=rx,g=x,o=rw", "261" }, new string
				[] { "u=rx,g=x,o=rx", "262" }, new string[] { "u=rx,g=x,o=r", "263" }, new string
				[] { "u=rx,g=x,o=wx", "264" }, new string[] { "u=rx,g=x,o=w", "265" }, new string
				[] { "u=rx,g=x,o=x", "266" }, new string[] { "u=rx,g=x,o=", "267" }, new string[
				] { "u=rx,g=,o=rwx", "270" }, new string[] { "u=rx,g=,o=rw", "271" }, new string
				[] { "u=rx,g=,o=rx", "272" }, new string[] { "u=rx,g=,o=r", "273" }, new string[
				] { "u=rx,g=,o=wx", "274" }, new string[] { "u=rx,g=,o=w", "275" }, new string[]
				 { "u=rx,g=,o=x", "276" }, new string[] { "u=rx,g=,o=", "277" }, new string[] { 
				"u=r,g=rwx,o=rwx", "300" }, new string[] { "u=r,g=rwx,o=rw", "301" }, new string
				[] { "u=r,g=rwx,o=rx", "302" }, new string[] { "u=r,g=rwx,o=r", "303" }, new string
				[] { "u=r,g=rwx,o=wx", "304" }, new string[] { "u=r,g=rwx,o=w", "305" }, new string
				[] { "u=r,g=rwx,o=x", "306" }, new string[] { "u=r,g=rwx,o=", "307" }, new string
				[] { "u=r,g=rw,o=rwx", "310" }, new string[] { "u=r,g=rw,o=rw", "311" }, new string
				[] { "u=r,g=rw,o=rx", "312" }, new string[] { "u=r,g=rw,o=r", "313" }, new string
				[] { "u=r,g=rw,o=wx", "314" }, new string[] { "u=r,g=rw,o=w", "315" }, new string
				[] { "u=r,g=rw,o=x", "316" }, new string[] { "u=r,g=rw,o=", "317" }, new string[
				] { "u=r,g=rx,o=rwx", "320" }, new string[] { "u=r,g=rx,o=rw", "321" }, new string
				[] { "u=r,g=rx,o=rx", "322" }, new string[] { "u=r,g=rx,o=r", "323" }, new string
				[] { "u=r,g=rx,o=wx", "324" }, new string[] { "u=r,g=rx,o=w", "325" }, new string
				[] { "u=r,g=rx,o=x", "326" }, new string[] { "u=r,g=rx,o=", "327" }, new string[
				] { "u=r,g=r,o=rwx", "330" }, new string[] { "u=r,g=r,o=rw", "331" }, new string
				[] { "u=r,g=r,o=rx", "332" }, new string[] { "u=r,g=r,o=r", "333" }, new string[
				] { "u=r,g=r,o=wx", "334" }, new string[] { "u=r,g=r,o=w", "335" }, new string[]
				 { "u=r,g=r,o=x", "336" }, new string[] { "u=r,g=r,o=", "337" }, new string[] { 
				"u=r,g=wx,o=rwx", "340" }, new string[] { "u=r,g=wx,o=rw", "341" }, new string[]
				 { "u=r,g=wx,o=rx", "342" }, new string[] { "u=r,g=wx,o=r", "343" }, new string[
				] { "u=r,g=wx,o=wx", "344" }, new string[] { "u=r,g=wx,o=w", "345" }, new string
				[] { "u=r,g=wx,o=x", "346" }, new string[] { "u=r,g=wx,o=", "347" }, new string[
				] { "u=r,g=w,o=rwx", "350" }, new string[] { "u=r,g=w,o=rw", "351" }, new string
				[] { "u=r,g=w,o=rx", "352" }, new string[] { "u=r,g=w,o=r", "353" }, new string[
				] { "u=r,g=w,o=wx", "354" }, new string[] { "u=r,g=w,o=w", "355" }, new string[]
				 { "u=r,g=w,o=x", "356" }, new string[] { "u=r,g=w,o=", "357" }, new string[] { 
				"u=r,g=x,o=rwx", "360" }, new string[] { "u=r,g=x,o=rw", "361" }, new string[] { 
				"u=r,g=x,o=rx", "362" }, new string[] { "u=r,g=x,o=r", "363" }, new string[] { "u=r,g=x,o=wx"
				, "364" }, new string[] { "u=r,g=x,o=w", "365" }, new string[] { "u=r,g=x,o=x", 
				"366" }, new string[] { "u=r,g=x,o=", "367" }, new string[] { "u=r,g=,o=rwx", "370"
				 }, new string[] { "u=r,g=,o=rw", "371" }, new string[] { "u=r,g=,o=rx", "372" }
				, new string[] { "u=r,g=,o=r", "373" }, new string[] { "u=r,g=,o=wx", "374" }, new 
				string[] { "u=r,g=,o=w", "375" }, new string[] { "u=r,g=,o=x", "376" }, new string
				[] { "u=r,g=,o=", "377" }, new string[] { "u=wx,g=rwx,o=rwx", "400" }, new string
				[] { "u=wx,g=rwx,o=rw", "401" }, new string[] { "u=wx,g=rwx,o=rx", "402" }, new 
				string[] { "u=wx,g=rwx,o=r", "403" }, new string[] { "u=wx,g=rwx,o=wx", "404" }, 
				new string[] { "u=wx,g=rwx,o=w", "405" }, new string[] { "u=wx,g=rwx,o=x", "406"
				 }, new string[] { "u=wx,g=rwx,o=", "407" }, new string[] { "u=wx,g=rw,o=rwx", "410"
				 }, new string[] { "u=wx,g=rw,o=rw", "411" }, new string[] { "u=wx,g=rw,o=rx", "412"
				 }, new string[] { "u=wx,g=rw,o=r", "413" }, new string[] { "u=wx,g=rw,o=wx", "414"
				 }, new string[] { "u=wx,g=rw,o=w", "415" }, new string[] { "u=wx,g=rw,o=x", "416"
				 }, new string[] { "u=wx,g=rw,o=", "417" }, new string[] { "u=wx,g=rx,o=rwx", "420"
				 }, new string[] { "u=wx,g=rx,o=rw", "421" }, new string[] { "u=wx,g=rx,o=rx", "422"
				 }, new string[] { "u=wx,g=rx,o=r", "423" }, new string[] { "u=wx,g=rx,o=wx", "424"
				 }, new string[] { "u=wx,g=rx,o=w", "425" }, new string[] { "u=wx,g=rx,o=x", "426"
				 }, new string[] { "u=wx,g=rx,o=", "427" }, new string[] { "u=wx,g=r,o=rwx", "430"
				 }, new string[] { "u=wx,g=r,o=rw", "431" }, new string[] { "u=wx,g=r,o=rx", "432"
				 }, new string[] { "u=wx,g=r,o=r", "433" }, new string[] { "u=wx,g=r,o=wx", "434"
				 }, new string[] { "u=wx,g=r,o=w", "435" }, new string[] { "u=wx,g=r,o=x", "436"
				 }, new string[] { "u=wx,g=r,o=", "437" }, new string[] { "u=wx,g=wx,o=rwx", "440"
				 }, new string[] { "u=wx,g=wx,o=rw", "441" }, new string[] { "u=wx,g=wx,o=rx", "442"
				 }, new string[] { "u=wx,g=wx,o=r", "443" }, new string[] { "u=wx,g=wx,o=wx", "444"
				 }, new string[] { "u=wx,g=wx,o=w", "445" }, new string[] { "u=wx,g=wx,o=x", "446"
				 }, new string[] { "u=wx,g=wx,o=", "447" }, new string[] { "u=wx,g=w,o=rwx", "450"
				 }, new string[] { "u=wx,g=w,o=rw", "451" }, new string[] { "u=wx,g=w,o=rx", "452"
				 }, new string[] { "u=wx,g=w,o=r", "453" }, new string[] { "u=wx,g=w,o=wx", "454"
				 }, new string[] { "u=wx,g=w,o=w", "455" }, new string[] { "u=wx,g=w,o=x", "456"
				 }, new string[] { "u=wx,g=w,o=", "457" }, new string[] { "u=wx,g=x,o=rwx", "460"
				 }, new string[] { "u=wx,g=x,o=rw", "461" }, new string[] { "u=wx,g=x,o=rx", "462"
				 }, new string[] { "u=wx,g=x,o=r", "463" }, new string[] { "u=wx,g=x,o=wx", "464"
				 }, new string[] { "u=wx,g=x,o=w", "465" }, new string[] { "u=wx,g=x,o=x", "466"
				 }, new string[] { "u=wx,g=x,o=", "467" }, new string[] { "u=wx,g=,o=rwx", "470"
				 }, new string[] { "u=wx,g=,o=rw", "471" }, new string[] { "u=wx,g=,o=rx", "472"
				 }, new string[] { "u=wx,g=,o=r", "473" }, new string[] { "u=wx,g=,o=wx", "474" }
				, new string[] { "u=wx,g=,o=w", "475" }, new string[] { "u=wx,g=,o=x", "476" }, 
				new string[] { "u=wx,g=,o=", "477" }, new string[] { "u=w,g=rwx,o=rwx", "500" }, 
				new string[] { "u=w,g=rwx,o=rw", "501" }, new string[] { "u=w,g=rwx,o=rx", "502"
				 }, new string[] { "u=w,g=rwx,o=r", "503" }, new string[] { "u=w,g=rwx,o=wx", "504"
				 }, new string[] { "u=w,g=rwx,o=w", "505" }, new string[] { "u=w,g=rwx,o=x", "506"
				 }, new string[] { "u=w,g=rwx,o=", "507" }, new string[] { "u=w,g=rw,o=rwx", "510"
				 }, new string[] { "u=w,g=rw,o=rw", "511" }, new string[] { "u=w,g=rw,o=rx", "512"
				 }, new string[] { "u=w,g=rw,o=r", "513" }, new string[] { "u=w,g=rw,o=wx", "514"
				 }, new string[] { "u=w,g=rw,o=w", "515" }, new string[] { "u=w,g=rw,o=x", "516"
				 }, new string[] { "u=w,g=rw,o=", "517" }, new string[] { "u=w,g=rx,o=rwx", "520"
				 }, new string[] { "u=w,g=rx,o=rw", "521" }, new string[] { "u=w,g=rx,o=rx", "522"
				 }, new string[] { "u=w,g=rx,o=r", "523" }, new string[] { "u=w,g=rx,o=wx", "524"
				 }, new string[] { "u=w,g=rx,o=w", "525" }, new string[] { "u=w,g=rx,o=x", "526"
				 }, new string[] { "u=w,g=rx,o=", "527" }, new string[] { "u=w,g=r,o=rwx", "530"
				 }, new string[] { "u=w,g=r,o=rw", "531" }, new string[] { "u=w,g=r,o=rx", "532"
				 }, new string[] { "u=w,g=r,o=r", "533" }, new string[] { "u=w,g=r,o=wx", "534" }
				, new string[] { "u=w,g=r,o=w", "535" }, new string[] { "u=w,g=r,o=x", "536" }, 
				new string[] { "u=w,g=r,o=", "537" }, new string[] { "u=w,g=wx,o=rwx", "540" }, 
				new string[] { "u=w,g=wx,o=rw", "541" }, new string[] { "u=w,g=wx,o=rx", "542" }
				, new string[] { "u=w,g=wx,o=r", "543" }, new string[] { "u=w,g=wx,o=wx", "544" }
				, new string[] { "u=w,g=wx,o=w", "545" }, new string[] { "u=w,g=wx,o=x", "546" }
				, new string[] { "u=w,g=wx,o=", "547" }, new string[] { "u=w,g=w,o=rwx", "550" }
				, new string[] { "u=w,g=w,o=rw", "551" }, new string[] { "u=w,g=w,o=rx", "552" }
				, new string[] { "u=w,g=w,o=r", "553" }, new string[] { "u=w,g=w,o=wx", "554" }, 
				new string[] { "u=w,g=w,o=w", "555" }, new string[] { "u=w,g=w,o=x", "556" }, new 
				string[] { "u=w,g=w,o=", "557" }, new string[] { "u=w,g=x,o=rwx", "560" }, new string
				[] { "u=w,g=x,o=rw", "561" }, new string[] { "u=w,g=x,o=rx", "562" }, new string
				[] { "u=w,g=x,o=r", "563" }, new string[] { "u=w,g=x,o=wx", "564" }, new string[
				] { "u=w,g=x,o=w", "565" }, new string[] { "u=w,g=x,o=x", "566" }, new string[] 
				{ "u=w,g=x,o=", "567" }, new string[] { "u=w,g=,o=rwx", "570" }, new string[] { 
				"u=w,g=,o=rw", "571" }, new string[] { "u=w,g=,o=rx", "572" }, new string[] { "u=w,g=,o=r"
				, "573" }, new string[] { "u=w,g=,o=wx", "574" }, new string[] { "u=w,g=,o=w", "575"
				 }, new string[] { "u=w,g=,o=x", "576" }, new string[] { "u=w,g=,o=", "577" }, new 
				string[] { "u=x,g=rwx,o=rwx", "600" }, new string[] { "u=x,g=rwx,o=rw", "601" }, 
				new string[] { "u=x,g=rwx,o=rx", "602" }, new string[] { "u=x,g=rwx,o=r", "603" }
				, new string[] { "u=x,g=rwx,o=wx", "604" }, new string[] { "u=x,g=rwx,o=w", "605"
				 }, new string[] { "u=x,g=rwx,o=x", "606" }, new string[] { "u=x,g=rwx,o=", "607"
				 }, new string[] { "u=x,g=rw,o=rwx", "610" }, new string[] { "u=x,g=rw,o=rw", "611"
				 }, new string[] { "u=x,g=rw,o=rx", "612" }, new string[] { "u=x,g=rw,o=r", "613"
				 }, new string[] { "u=x,g=rw,o=wx", "614" }, new string[] { "u=x,g=rw,o=w", "615"
				 }, new string[] { "u=x,g=rw,o=x", "616" }, new string[] { "u=x,g=rw,o=", "617" }
				, new string[] { "u=x,g=rx,o=rwx", "620" }, new string[] { "u=x,g=rx,o=rw", "621"
				 }, new string[] { "u=x,g=rx,o=rx", "622" }, new string[] { "u=x,g=rx,o=r", "623"
				 }, new string[] { "u=x,g=rx,o=wx", "624" }, new string[] { "u=x,g=rx,o=w", "625"
				 }, new string[] { "u=x,g=rx,o=x", "626" }, new string[] { "u=x,g=rx,o=", "627" }
				, new string[] { "u=x,g=r,o=rwx", "630" }, new string[] { "u=x,g=r,o=rw", "631" }
				, new string[] { "u=x,g=r,o=rx", "632" }, new string[] { "u=x,g=r,o=r", "633" }, 
				new string[] { "u=x,g=r,o=wx", "634" }, new string[] { "u=x,g=r,o=w", "635" }, new 
				string[] { "u=x,g=r,o=x", "636" }, new string[] { "u=x,g=r,o=", "637" }, new string
				[] { "u=x,g=wx,o=rwx", "640" }, new string[] { "u=x,g=wx,o=rw", "641" }, new string
				[] { "u=x,g=wx,o=rx", "642" }, new string[] { "u=x,g=wx,o=r", "643" }, new string
				[] { "u=x,g=wx,o=wx", "644" }, new string[] { "u=x,g=wx,o=w", "645" }, new string
				[] { "u=x,g=wx,o=x", "646" }, new string[] { "u=x,g=wx,o=", "647" }, new string[
				] { "u=x,g=w,o=rwx", "650" }, new string[] { "u=x,g=w,o=rw", "651" }, new string
				[] { "u=x,g=w,o=rx", "652" }, new string[] { "u=x,g=w,o=r", "653" }, new string[
				] { "u=x,g=w,o=wx", "654" }, new string[] { "u=x,g=w,o=w", "655" }, new string[]
				 { "u=x,g=w,o=x", "656" }, new string[] { "u=x,g=w,o=", "657" }, new string[] { 
				"u=x,g=x,o=rwx", "660" }, new string[] { "u=x,g=x,o=rw", "661" }, new string[] { 
				"u=x,g=x,o=rx", "662" }, new string[] { "u=x,g=x,o=r", "663" }, new string[] { "u=x,g=x,o=wx"
				, "664" }, new string[] { "u=x,g=x,o=w", "665" }, new string[] { "u=x,g=x,o=x", 
				"666" }, new string[] { "u=x,g=x,o=", "667" }, new string[] { "u=x,g=,o=rwx", "670"
				 }, new string[] { "u=x,g=,o=rw", "671" }, new string[] { "u=x,g=,o=rx", "672" }
				, new string[] { "u=x,g=,o=r", "673" }, new string[] { "u=x,g=,o=wx", "674" }, new 
				string[] { "u=x,g=,o=w", "675" }, new string[] { "u=x,g=,o=x", "676" }, new string
				[] { "u=x,g=,o=", "677" }, new string[] { "u=,g=rwx,o=rwx", "700" }, new string[
				] { "u=,g=rwx,o=rw", "701" }, new string[] { "u=,g=rwx,o=rx", "702" }, new string
				[] { "u=,g=rwx,o=r", "703" }, new string[] { "u=,g=rwx,o=wx", "704" }, new string
				[] { "u=,g=rwx,o=w", "705" }, new string[] { "u=,g=rwx,o=x", "706" }, new string
				[] { "u=,g=rwx,o=", "707" }, new string[] { "u=,g=rw,o=rwx", "710" }, new string
				[] { "u=,g=rw,o=rw", "711" }, new string[] { "u=,g=rw,o=rx", "712" }, new string
				[] { "u=,g=rw,o=r", "713" }, new string[] { "u=,g=rw,o=wx", "714" }, new string[
				] { "u=,g=rw,o=w", "715" }, new string[] { "u=,g=rw,o=x", "716" }, new string[] 
				{ "u=,g=rw,o=", "717" }, new string[] { "u=,g=rx,o=rwx", "720" }, new string[] { 
				"u=,g=rx,o=rw", "721" }, new string[] { "u=,g=rx,o=rx", "722" }, new string[] { 
				"u=,g=rx,o=r", "723" }, new string[] { "u=,g=rx,o=wx", "724" }, new string[] { "u=,g=rx,o=w"
				, "725" }, new string[] { "u=,g=rx,o=x", "726" }, new string[] { "u=,g=rx,o=", "727"
				 }, new string[] { "u=,g=r,o=rwx", "730" }, new string[] { "u=,g=r,o=rw", "731" }
				, new string[] { "u=,g=r,o=rx", "732" }, new string[] { "u=,g=r,o=r", "733" }, new 
				string[] { "u=,g=r,o=wx", "734" }, new string[] { "u=,g=r,o=w", "735" }, new string
				[] { "u=,g=r,o=x", "736" }, new string[] { "u=,g=r,o=", "737" }, new string[] { 
				"u=,g=wx,o=rwx", "740" }, new string[] { "u=,g=wx,o=rw", "741" }, new string[] { 
				"u=,g=wx,o=rx", "742" }, new string[] { "u=,g=wx,o=r", "743" }, new string[] { "u=,g=wx,o=wx"
				, "744" }, new string[] { "u=,g=wx,o=w", "745" }, new string[] { "u=,g=wx,o=x", 
				"746" }, new string[] { "u=,g=wx,o=", "747" }, new string[] { "u=,g=w,o=rwx", "750"
				 }, new string[] { "u=,g=w,o=rw", "751" }, new string[] { "u=,g=w,o=rx", "752" }
				, new string[] { "u=,g=w,o=r", "753" }, new string[] { "u=,g=w,o=wx", "754" }, new 
				string[] { "u=,g=w,o=w", "755" }, new string[] { "u=,g=w,o=x", "756" }, new string
				[] { "u=,g=w,o=", "757" }, new string[] { "u=,g=x,o=rwx", "760" }, new string[] 
				{ "u=,g=x,o=rw", "761" }, new string[] { "u=,g=x,o=rx", "762" }, new string[] { 
				"u=,g=x,o=r", "763" }, new string[] { "u=,g=x,o=wx", "764" }, new string[] { "u=,g=x,o=w"
				, "765" }, new string[] { "u=,g=x,o=x", "766" }, new string[] { "u=,g=x,o=", "767"
				 }, new string[] { "u=,g=,o=rwx", "770" }, new string[] { "u=,g=,o=rw", "771" }, 
				new string[] { "u=,g=,o=rx", "772" }, new string[] { "u=,g=,o=r", "773" }, new string
				[] { "u=,g=,o=wx", "774" }, new string[] { "u=,g=,o=w", "775" }, new string[] { 
				"u=,g=,o=x", "776" }, new string[] { "u=,g=,o=", "777" } };
			for (int i = 0; i < symbolic.Length; i += 2)
			{
				conf.Set(FsPermission.UmaskLabel, symbolic[i][0]);
				short val = short.ValueOf(symbolic[i][1], 8);
				Assert.Equal(val, FsPermission.GetUMask(conf).ToShort());
			}
		}

		public virtual void TestBadUmasks()
		{
			Configuration conf = new Configuration();
			foreach (string b in new string[] { "1777", "22", "99", "foo", string.Empty })
			{
				conf.Set(FsPermission.UmaskLabel, b);
				try
				{
					FsPermission.GetUMask(conf);
					Fail("Shouldn't have been able to parse bad umask");
				}
				catch (ArgumentException iae)
				{
					Assert.True("Exception should specify parsing error and invalid umask: "
						 + iae.Message, IsCorrectExceptionMessage(iae.Message, b));
				}
			}
		}

		private bool IsCorrectExceptionMessage(string msg, string umask)
		{
			return msg.Contains("Unable to parse") && msg.Contains(umask) && msg.Contains("octal or symbolic"
				);
		}

		// Ensure that when the deprecated decimal umask key is used, it is correctly
		// parsed as such and converted correctly to an FsPermission value
		public virtual void TestDeprecatedUmask()
		{
			Configuration conf = new Configuration();
			conf.Set(FsPermission.DeprecatedUmaskLabel, "302");
			// 302 = 0456
			FsPermission umask = FsPermission.GetUMask(conf);
			Assert.Equal(0x12e, umask.ToShort());
		}
	}
}
