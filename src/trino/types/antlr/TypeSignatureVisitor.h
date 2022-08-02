
    #include <boost/algorithm/string.hpp>
    #include "types/TypeSignatureTypeConverter.h"
      

// Generated from TypeSignature.g4 by ANTLR 4.9.3

#pragma once


#include "antlr4-runtime.h"
#include "TypeSignatureParser.h"


namespace datalight::trino::type {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by TypeSignatureParser.
 */
class  TypeSignatureVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by TypeSignatureParser.
   */
    virtual antlrcpp::Any visitStart(TypeSignatureParser::StartContext *context) = 0;

    virtual antlrcpp::Any visitType_spec(TypeSignatureParser::Type_specContext *context) = 0;

    virtual antlrcpp::Any visitNamed_type(TypeSignatureParser::Named_typeContext *context) = 0;

    virtual antlrcpp::Any visitType(TypeSignatureParser::TypeContext *context) = 0;

    virtual antlrcpp::Any visitSimple_type(TypeSignatureParser::Simple_typeContext *context) = 0;

    virtual antlrcpp::Any visitVariable_type(TypeSignatureParser::Variable_typeContext *context) = 0;

    virtual antlrcpp::Any visitDecimal_type(TypeSignatureParser::Decimal_typeContext *context) = 0;

    virtual antlrcpp::Any visitType_list(TypeSignatureParser::Type_listContext *context) = 0;

    virtual antlrcpp::Any visitRow_type(TypeSignatureParser::Row_typeContext *context) = 0;

    virtual antlrcpp::Any visitMap_type(TypeSignatureParser::Map_typeContext *context) = 0;

    virtual antlrcpp::Any visitArray_type(TypeSignatureParser::Array_typeContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(TypeSignatureParser::IdentifierContext *context) = 0;


};

}  // namespace datalight::trino::type
