/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_beng_ft_parser.h"
#include "data_plane/fts/ob_fts_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
namespace
{
// thread_local 复用池：跨 segment() 调用复用同一个 parser 实例（含 english_analyzer_ 的
// tokenizer + normalizer pipeline）。线程本地，天然规避跨线程竞争。key 为 cs 指针，
// benchmark 只会有 utf8mb4 一种。
struct ObBengFTParserReusePool
{
  common::ObArenaAllocator arena_;
  ObBEngFTParser *parser_ = nullptr;
  const ObCharsetInfo *cs_ = nullptr;

  ~ObBengFTParserReusePool()
  {
    if (OB_NOT_NULL(parser_)) {
      parser_->~ObBEngFTParser();
      parser_ = nullptr;
    }
  }
};
thread_local ObBengFTParserReusePool g_beng_parser_pool;
} // namespace

int ObBEngFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  ObDatum token;
  int64_t token_freq = 0;
  char *buf = nullptr;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(token_stream_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(token_stream_->get_next(token, token_freq))) {
    if (OB_ITER_END != ret) {
    }
  } else if (OB_ISNULL(token.ptr_) || OB_UNLIKELY(0 >= token.len_ || 0 >= token_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(token_alloc_) || OB_ISNULL(buf = static_cast<char *>(token_alloc_->alloc(token.len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, token.ptr_, token.len_);
    word = buf;
    word_len = token.len_;
    char_len = token.len_;
    word_freq = token_freq;
    LOG_DEBUG("succeed to add word", K(ObString(word_len, word)), K(word_freq));
  }
  return ret;
}

int ObBEngFTParser::init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < param->ft_length_)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    doc_.set_string(param->fulltext_, param->ft_length_);
    token_alloc_ = param->allocator_;
    analysis_ctx_.cs_ = param->cs_;
    analysis_ctx_.filter_stopword_ = false;
    analysis_ctx_.need_grouping_ = false;
    // 复用场景下 parser 由 thread_local 常驻 arena 构造，pipeline 必须用该 arena 分配，
    // 否则 per-query allocator 释放后 token_stream 对象会悬空。
    if (OB_FAIL(english_analyzer_.init(analysis_ctx_, allocator_))) {
    } else if (OB_FAIL(segment(doc_, token_stream_))) {
    } else if (OB_ISNULL(token_stream_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObBEngFTParser::reuse_init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < param->ft_length_)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    // 复用 english_analyzer_ 的 pipeline（tokenizer + normalizer 均已构造），仅更新 per-document
    // 状态并重新 analyze；analyze() 内部会 reuse() 整条 pipeline + open() tokenizer。
    doc_.set_string(param->fulltext_, param->ft_length_);
    token_alloc_ = param->allocator_;
    analysis_ctx_.cs_ = param->cs_;
    analysis_ctx_.filter_stopword_ = false;
    analysis_ctx_.need_grouping_ = false;
    if (OB_FAIL(segment(doc_, token_stream_))) {
    } else if (OB_ISNULL(token_stream_)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObBEngFTParser::segment(
    const common::ObDatum &doc,
    share::ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(doc.ptr_) || OB_UNLIKELY(0 >= doc.len_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < doc.len_)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(english_analyzer_.analyze(doc, token_stream))) {
  }
  return ret;
}

void ObBEngFTParser::reset()
{
  analysis_ctx_.reset();
  english_analyzer_.reset();
  doc_.reset();
  token_stream_ = nullptr;
  token_alloc_ = nullptr;
  is_inited_ = false;
}

ObBasicEnglishFTParserDesc::ObBasicEnglishFTParserDesc() {}

int ObBasicEnglishFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObBengFTParserReusePool &pool = g_beng_parser_pool;
    // 命中复用池：仅更新 per-document 状态并重新 analyze，跳过 parser/pipeline 重建。
    if (OB_NOT_NULL(pool.parser_) && pool.cs_ == param->cs_) {
      if (OB_FAIL(pool.parser_->reuse_init(param))) {
        // 复用失败（罕见）：销毁后走完整重建分支。
        pool.parser_->~ObBEngFTParser();
        pool.parser_ = nullptr;
      } else {
        iter = pool.parser_;
      }
    }
    // 未命中或复用失败：完整 init 并缓存。
    if (OB_SUCC(ret) && OB_ISNULL(iter)) {
      ObBEngFTParser *parser = nullptr;
      if (OB_ISNULL(parser = OB_NEWx(ObBEngFTParser, &pool.arena_, pool.arena_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(parser->init(param))) {
        OB_DELETEx(ObBEngFTParser, &pool.arena_, parser);
      } else {
        pool.parser_ = parser;
        pool.cs_ = param->cs_;
        iter = parser;
      }
    }
  }

  return ret;
}

void ObBasicEnglishFTParserDesc::free_token_iter(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  UNUSED(param);
  // 归还语义：english_analyzer_ 的 pipeline 留在复用池继续用，这里仅置空引用。
  iter = nullptr;
}

int ObBasicEnglishFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_min_max_word();
  flag.set_stop_word();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
