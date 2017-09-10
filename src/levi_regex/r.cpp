#include "r.h"

namespace LeviDB {
    namespace Regex {
        R & R::operator=(const R & another) noexcept {
            if (another._r != nullptr) {
                assert(another._pattern.empty());
                _r = std::make_unique<R>(*another._r);
            } else {
                assert(!another._pattern.empty());
                _pattern = another._pattern;
                _pattern_to = another._pattern_to;
            }

            _num_from = another._num_from;
            _num_to = another._num_to;

            if (another._other != nullptr) {
                _other = std::make_unique<R>(*another._other);
            }
            _relation = another._relation;
            _mode = another._mode;
            return *this;
        }

        R R::operator&(R another) const & noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(*this);
            self->_relation = AND;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        }

        R R::operator&(R another) && noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(std::move(*this));
            self->_relation = AND;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        }

        R R::operator|(R another) const & noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(*this);
            self->_relation = OR;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        };

        R R::operator|(R another) && noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(std::move(*this));
            self->_relation = OR;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        };

        R R::operator^(R another) const & noexcept {
            assert(_relation == NONE);
            auto self = *this;
            auto part = (self | another);
            return std::move(part) & (~(std::move(self) & std::move(another)));
        };

        R R::operator^(R another) && noexcept {
            assert(_relation == NONE);
            auto self = std::move(*this);
            auto part = (self | another);
            return std::move(part) & (~(std::move(self) & std::move(another)));
        };

        R R::operator~() const & noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(*this);
            self->_relation = INVERT;
            return R(std::move(self));
        };

        R R::operator~() && noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(std::move(*this));
            self->_relation = INVERT;
            return R(std::move(self));
        };

        R R::operator<<(R another) const & noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(*this);
            self->_relation = NEXT;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        };

        R R::operator<<(R another) && noexcept {
            assert(_relation == NONE);
            auto self = std::make_unique<R>(std::move(*this));
            self->_relation = NEXT;
            self->_other = std::make_unique<R>(std::move(another));
            return R(std::move(self));
        };

        R R::operator<<(std::string another) const & noexcept {
            assert(_relation == NONE);
            return (*this) << R(std::move(another));
        };

        R R::operator<<(std::string another) && noexcept {
            assert(_relation == NONE);
            return std::move(*this) << R(std::move(another));
        };

        bool R::possible(const USR & input) const {
            auto it = imatch(input, {0, 0, false});
            while (it->valid()) {
                if (it->item().isContinue() || it->item().isSuccess()) {
                    return true;
                }
            }
            cacheClear();
            return false;
        }

        bool R::match(const USR & input) const {
            auto it = imatch(input, {0, 0, false});
            while (it->valid()) {
                if (it->item().isSuccess()) {
                    return true;
                }
            }
            cacheClear();
            return false;
        }
    }
}