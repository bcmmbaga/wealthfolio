import { useMutation } from "@tanstack/react-query";
import { syncDseBrokerData } from "../services/broker-service";
import { toast } from "@wealthfolio/ui/components/ui/use-toast";

/**
 * Hook to trigger DSE broker data sync.
 * The sync runs in the background and results are handled via
 * global event listeners (SSE events trigger toasts and query invalidation).
 */
export function useSyncDseBrokerData() {
  return useMutation({
    mutationFn: syncDseBrokerData,
    onSuccess: () => {
      toast.loading("Syncing DSE broker data...", { id: "dse-broker-sync-start" });
    },
    onError: (error) => {
      toast.error(
        `Failed to start DSE sync: ${error instanceof Error ? error.message : "Unknown error"}`,
      );
    },
  });
}
