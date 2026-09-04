/**
 * PrimeAdapter - Prime Agent ACP implementation of the generic provider contract.
 *
 * @module PrimeAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface PrimeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "prime";
}

export class PrimeAdapter extends ServiceMap.Service<PrimeAdapter, PrimeAdapterShape>()(
  "synara/provider/Services/PrimeAdapter",
) {}
