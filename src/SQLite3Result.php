<?php declare(strict_types=1);

/**
 * This file is part of Reymon.
 * Reymon is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * Reymon is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    AhJ <AmirHosseinJafari8228@gmail.com>
 * @copyright 2023-2024 AhJ <AmirHosseinJafari8228@gmail.com>
 * @license   https://choosealicense.com/licenses/gpl-3.0/ GPLv3
 */

namespace Amp\SQLite3;

use Amp\Sql\SqlResult;

/**
 * Recursive template types currently not supported, list<mixed> should be list<TFieldType>.
 * @psalm-type TFieldType = list<mixed>|scalar|null
 * @psalm-type TRowType = array<string, TFieldType>
 * @extends SqlResult<TFieldType>
 */
interface SQLite3Result extends SqlResult
{
    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?self;

    /**
     * @return int|null Insert ID of the last auto increment row if applicable to the result or null if no ID
     *                  is available.
     */
    public function getLastInsertId(): ?int;
}
