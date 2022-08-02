
    #include <boost/algorithm/string.hpp>
    #include "types/TypeSignatureTypeConverter.h"
      

// Generated from TypeSignature.g4 by ANTLR 4.9.3

#pragma once


#include "antlr4-runtime.h"
#include "TypeSignatureVisitor.h"


namespace datalight::trino::type {

/**
 * This class provides an empty implementation of TypeSignatureVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  TypeSignatureBaseVisitor : public TypeSignatureVisitor {
public:

  virtual antlrcpp::Any visitStart(TypeSignatureParser::StartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitType_spec(TypeSignatureParser::Type_specContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNamed_type(TypeSignatureParser::Named_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitType(TypeSignatureParser::TypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSimple_type(TypeSignatureParser::Simple_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVariable_type(TypeSignatureParser::Variable_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDecimal_type(TypeSignatureParser::Decimal_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitType_list(TypeSignatureParser::Type_listContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRow_type(TypeSignatureParser::Row_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMap_type(TypeSignatureParser::Map_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArray_type(TypeSignatureParser::Array_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIdentifier(TypeSignatureParser::IdentifierContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace datalight::trino::type
