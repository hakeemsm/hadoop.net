

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JBoolean : JType
	{
		internal class JavaBoolean : JType.JavaType
		{
			internal JavaBoolean(JBoolean _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				cb.Append(Consts.RioPrefix + "ret = (" + fname + " == " + other + ")? 0 : (" + fname
					 + "?1:-1);\n");
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.BoolTypeID";
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = (" + fname + ")?0:1;\n");
			}

			// In Binary format, boolean is written as byte. true = 1, false = 0
			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("if (" + l + "<1) {\n");
				cb.Append("throw new java.io.IOException(\"Boolean is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append(s + "++; " + l + "--;\n");
				cb.Append("}\n");
			}

			// In Binary format, boolean is written as byte. true = 1, false = 0
			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("if (l1<1 || l2<1) {\n");
				cb.Append("throw new java.io.IOException(\"Boolean is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append("if (b1[s1] != b2[s2]) {\n");
				cb.Append("return (b1[s1]<b2[s2])? -1 : 0;\n");
				cb.Append("}\n");
				cb.Append("s1++; s2++; l1--; l2--;\n");
				cb.Append("}\n");
			}

			private readonly JBoolean _enclosing;
		}

		internal class CppBoolean : JType.CppType
		{
			internal CppBoolean(JBoolean _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BOOL)";
			}

			private readonly JBoolean _enclosing;
		}

		/// <summary>Creates a new instance of JBoolean</summary>
		public JBoolean()
		{
			SetJavaType(new JBoolean.JavaBoolean(this));
			SetCppType(new JBoolean.CppBoolean(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "z";
		}
	}
}
